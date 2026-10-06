//! Turning a workload's dependency graph into a verdict.
//!
//! A workload (list-append, rw-register) contributes data edges and any
//! anomalies it can see directly (aborted reads, lost updates...). This module
//! adds process or real-time order when the requested models need it, then
//! looks for cycles one strongly connected component at a time:
//!
//! 1. cheap existence tests: does the subgraph a model forbids cycles in have
//!    a nontrivial SCC at all? This alone bounds which anomalies are possible;
//! 2. a typed search for the most severe anomaly first, skipping anything
//!    already implied by what was found.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Write as _;
use std::time::{Duration, Instant};

use serde::Serialize;
use serde_json::{json, Value as Json};

use crate::graph::{
    any_cycle, classify, edge_type, find_cycle, rel_name, CycleSpec, Graph, GraphBuilder, RwMode, Search, Step, PROCESS,
    REALTIME, RW, WR, WW,
};
use crate::history::{History, Id, Mop, OpType, ReadValue};
use crate::model;

/// One anomaly, with a human explanation and machine-readable detail.
#[derive(Debug, Clone, Serialize)]
pub struct Anomaly {
    #[serde(rename = "type")]
    pub kind: String,
    /// History indices of the operations involved.
    pub ops: Vec<u64>,
    pub explanation: String,
    pub detail: Json,
}

impl Anomaly {
    pub fn new(kind: &str, ops: Vec<u64>, explanation: String, detail: Json) -> Anomaly {
        Anomaly { kind: kind.into(), ops, explanation, detail }
    }
}

/// Explains why one transaction must precede another along an edge.
pub trait Explain {
    fn explain(&self, a: &Analysis, from: usize, to: usize, rel: u8) -> String;
}

/// What a workload learned from a history.
pub struct Analysis<'h> {
    h: &'h History,
    vertex: HashMap<usize, u32>,
    op_of: Vec<usize>,
    pub anomalies: BTreeMap<String, Vec<Anomaly>>,
    /// Set when the history can't be analyzed at all; the verdict is unknown.
    pub fatal: bool,
    pub data: Option<GraphBuilder>,
    pub explainer: Option<Box<dyn Explain>>,
}

impl<'h> Analysis<'h> {
    pub fn new(h: &'h History) -> Analysis<'h> {
        Analysis {
            h,
            vertex: HashMap::new(),
            op_of: Vec::new(),
            anomalies: BTreeMap::new(),
            fatal: false,
            data: None,
            explainer: None,
        }
    }

    pub fn history(&self) -> &'h History {
        self.h
    }

    /// The graph vertex for the completion op at position `pos`.
    pub fn vertex(&mut self, pos: usize) -> u32 {
        let next = self.op_of.len() as u32;
        let v = *self.vertex.entry(pos).or_insert(next);
        if v == next {
            self.op_of.push(pos);
        }
        v
    }

    pub fn push(&mut self, a: Anomaly) {
        self.anomalies.entry(a.kind.clone()).or_default().push(a);
    }

    pub fn unknown(&mut self, kind: &str, msg: impl Into<String>) {
        self.fatal = true;
        self.push(Anomaly::new(kind, vec![], msg.into(), Json::Null));
    }

    pub fn name(&self, pos: usize) -> String {
        format!("T{}", self.h.ops[pos].index)
    }

    pub fn scalar(&self, id: Id) -> Json {
        scalar_json(self.h, id)
    }

    pub fn list(&self, v: &[Id]) -> String {
        self.list_json(v).to_string()
    }

    pub fn list_json(&self, v: &[Id]) -> Json {
        Json::Array(v.iter().map(|e| self.scalar(*e)).collect())
    }

    pub fn op_json(&self, pos: usize) -> Json {
        op_json(self.h, pos)
    }

    /// Explains a process or real-time edge.
    pub fn explain_order(&self, from: usize, to: usize, rel: u8) -> String {
        let (a, b) = (&self.h.ops[from], &self.h.ops[to]);
        let (an, bn) = (self.name(from), self.name(to));
        if rel == PROCESS {
            return format!("process {} executed {an} before {bn}", a.process.unwrap_or(0));
        }
        let inv = self.h.pair[to].map(|i| &self.h.ops[i]);
        let gap = match (a.time, inv.and_then(|o| o.time)) {
            (Some(t1), Some(t2)) if t2 > t1 => format!(" {:.3} seconds", (t2 - t1) as f64 / 1e9),
            _ => String::new(),
        };
        let inv_index = inv.map_or(b.index, |o| o.index);
        format!("{an} completed at index {},{gap} before the invocation of {bn}, at index {inv_index}", a.index)
    }
}

pub(crate) fn scalar_json(h: &History, id: Id) -> Json {
    match h.show(id) {
        crate::history::Scalar::Int(i) => json!(i),
        crate::history::Scalar::Str(s) => json!(s),
    }
}

pub(crate) fn mop_json(h: &History, m: &Mop) -> Json {
    let s = |id| scalar_json(h, id);
    match m {
        Mop::Append { key, value } => json!(["append", s(*key), s(*value)]),
        Mop::Write { key, value } => json!(["w", s(*key), s(*value)]),
        Mop::Read { key, value } => json!([
            "r",
            s(*key),
            match value {
                ReadValue::Nil => Json::Null,
                ReadValue::Scalar(v) => s(*v),
                ReadValue::List(xs) => Json::Array(xs.iter().map(|x| s(*x)).collect()),
            }
        ]),
    }
}

pub(crate) fn op_json(h: &History, pos: usize) -> Json {
    let op = &h.ops[pos];
    let kind = match op.kind {
        OpType::Invoke => "invoke",
        OpType::Ok => "ok",
        OpType::Fail => "fail",
        OpType::Info => "info",
    };
    json!({
        "index": op.index,
        "type": kind,
        "process": op.process,
        "value": op.value.iter().map(|m| mop_json(h, m)).collect::<Vec<_>>(),
    })
}

/// Which workload produced the history.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Workload {
    ListAppend,
    RwRegister,
}

#[derive(Clone, Debug)]
pub struct Opts {
    /// Consistency models the history is expected to satisfy.
    pub models: Vec<String>,
    /// Extra anomalies to treat as failures.
    pub anomalies: Vec<String>,
    /// Budget for cycle search in each strongly connected component.
    pub timeout: Duration,
    /// rw-register only: assume writes follow reads within a transaction.
    pub wfr_keys: bool,
}

impl Default for Opts {
    fn default() -> Opts {
        Opts { models: vec!["strict-serializable".into()], anomalies: vec![], timeout: Duration::from_millis(1000), wfr_keys: false }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Valid {
    True,
    False,
    Unknown,
}

impl Serialize for Valid {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Valid::True => s.serialize_bool(true),
            Valid::False => s.serialize_bool(false),
            Valid::Unknown => s.serialize_str("unknown"),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Report {
    pub valid: Valid,
    pub anomaly_types: Vec<String>,
    pub anomalies: BTreeMap<String, Vec<Anomaly>>,
    /// The weakest models these anomalies rule out...
    pub not: Vec<String>,
    /// ...and every stronger model, also ruled out.
    pub also_not: Vec<String>,
}

const UNKNOWN_TYPES: [&str; 2] = ["empty-transaction-graph", "cycle-search-timeout"];

/// Cycle anomaly specs, most severe first; then process, then real-time
/// variants.
fn cycle_specs() -> Vec<CycleSpec> {
    let base = [
        ("G0", WW, RwMode::None, false),
        ("G1c", WW | WR, RwMode::None, true),
        ("G-single-item", WW | WR, RwMode::Single, false),
        ("G-nonadjacent-item", WW | WR, RwMode::Nonadjacent, false),
        ("G2-item", WW | WR, RwMode::Adjacent, false),
    ];
    let mut out = Vec::new();
    for (suffix, extra) in [("", 0), ("-process", PROCESS), ("-realtime", REALTIME)] {
        for (name, allowed, rw, need_wr) in base {
            let name: &'static str = Box::leak(format!("{name}{suffix}").into_boxed_str());
            out.push(CycleSpec { name, allowed: allowed | extra, rw, need_wr, need: extra });
        }
    }
    out
}

/// Subgraphs that must be acyclic for each model: `(model, types, extension)`.
/// With an extension, edges are `types` plus `types` followed by one `rw`.
const CYCLE_EXISTS: [(&str, &str, u8, bool); 13] = [
    ("PL-1", "read-uncommitted", WW, false),
    ("PL-2", "read-committed", WW | WR, false),
    ("PL-SI", "snapshot-isolation", WW | WR, true),
    ("PL-2.99", "repeatable-read", WW | WR | RW, false),
    ("PL-3", "serializable", WW | WR | RW, false),
    ("strong-session-PL-1", "strong-session-read-uncommitted", WW | PROCESS, false),
    ("strong-session-PL-2", "strong-session-read-committed", WW | WR | PROCESS, false),
    ("strong-session-snapshot-isolation", "strong-session-snapshot-isolation", WW | WR | PROCESS, true),
    ("strong-session-serializable", "strong-session-serializable", WW | WR | RW | PROCESS, false),
    ("strong-PL-1", "strong-read-uncommitted", WW | REALTIME, false),
    ("strong-PL-2", "strong-read-committed", WW | WR | REALTIME, false),
    ("strong-snapshot-isolation", "strong-snapshot-isolation", WW | WR | REALTIME, true),
    ("PL-SS", "strict-serializable", WW | WR | RW | REALTIME, false),
];

/// Checks a history against the given options.
pub fn check(h: &History, workload: Workload, opts: &Opts) -> Report {
    let mut a = match workload {
        Workload::ListAppend => crate::list_append::analyze(h),
        Workload::RwRegister => crate::rw_register::analyze_with(h, crate::rw_register::Options { wfr_keys: opts.wfr_keys }),
    };
    let models: Vec<&str> = opts.models.iter().map(String::as_str).collect();
    let extra: Vec<&str> = opts.anomalies.iter().map(String::as_str).collect();
    let mut prohibited = model::prohibited_by(&models);
    prohibited.extend(model::implying(&extra));
    let mut reportable = prohibited.clone();
    reportable.extend(UNKNOWN_TYPES.iter().map(|s| s.to_string()));

    if !a.fatal {
        cycles(h, &mut a, opts, &reportable);
    }
    report(a.anomalies, &prohibited, &reportable)
}

fn cycles(h: &History, a: &mut Analysis, opts: &Opts, reportable: &BTreeSet<String>) {
    let mut g = a.data.take().unwrap_or_default();
    let wants = |suffix: &str| reportable.iter().any(|t| t.starts_with('G') && t.ends_with(suffix));
    if wants("-realtime") {
        realtime_edges(h, a, &mut g);
    } else if wants("-process") {
        process_edges(h, a, &mut g);
    }
    let g = g.finish(a.op_of.len());
    let nodes: Vec<u32> = (0..g.len() as u32).collect();
    if g.adj.iter().all(Vec::is_empty) {
        a.push(Anomaly::new(
            "empty-transaction-graph",
            vec![],
            "no dependencies could be inferred between transactions, so the history says nothing about isolation".into(),
            Json::Null,
        ));
        return;
    }
    let specs = cycle_specs();
    let present: u8 = g.adj.iter().flatten().fold(0, |acc, &(_, r)| acc | edge_type(r));
    for scc in g.sccs(&nodes, |_| true) {
        scc_cases(&g, &scc, &specs, present, a, opts);
    }
}

fn scc_cases(g: &Graph, scc: &[u32], specs: &[CycleSpec], present: u8, a: &mut Analysis, opts: &Opts) {
    let deadline = Instant::now() + opts.timeout;
    let ops = |vs: &[u32], a: &Analysis| -> Vec<u64> { vs.iter().map(|&v| a.h.ops[a.op_of[v as usize]].index).collect() };

    // 1. Which models' forbidden subgraphs are cyclic?
    let mut exists: Vec<Anomaly> = Vec::new();
    let mut ruled_out: BTreeSet<String> = BTreeSet::new();
    for (canon, friendly, types, ext) in CYCLE_EXISTS {
        if ruled_out.contains(friendly) {
            continue;
        }
        let comps = if ext { extension_sccs(g, scc, types) } else { g.sccs(scc, |r| r & types != 0) };
        if let Some(c) = comps.first() {
            let kinds: Vec<&str> = [WW, WR, RW, PROCESS, REALTIME].into_iter().filter(|r| types & r != 0).map(rel_name).collect();
            let shape = if ext { format!("{} plus one rw hop", kinds.join("/")) } else { kinds.join("/") };
            exists.push(Anomaly::new(
                &format!("{canon}-cycle-exists"),
                ops(c, a),
                format!(
                    "a cycle of {shape} edges exists among these {} transactions, so the history is not {friendly}",
                    c.len()
                ),
                json!({"not": friendly, "scc_size": c.len()}),
            ));
            ruled_out.extend(model::stronger_models(&[friendly]));
        }
    }
    let possible: Vec<&str> =
        CYCLE_EXISTS.iter().map(|c| c.1).filter(|m| !ruled_out.contains(*m)).collect();
    let mut skip: BTreeSet<String> = model::prohibited_by(&possible);

    // 2. Typed search, most severe first.
    let mut found: Vec<Anomaly> = Vec::new();
    for spec in specs {
        if skip.contains(spec.name) || (spec.need != 0 && present & spec.need == 0) {
            continue;
        }
        match find_cycle(g, scc, spec, deadline) {
            Search::Found(steps) => {
                debug_assert_eq!(classify(&steps), spec.name);
                found.push(cycle_anomaly(a, spec.name, &steps));
                skip.extend(model::implied(&[spec.name]));
            }
            Search::NotFound => {}
            Search::TimedOut => {
                let steps = any_cycle(g, scc);
                found.push(Anomaly::new(
                    "cycle-search-timeout",
                    ops(scc, a),
                    format!(
                        "gave up looking for {} after {:?} in a component of {} transactions; reporting some cycle instead",
                        spec.name,
                        opts.timeout,
                        scc.len()
                    ),
                    json!({"anomaly_spec_type": spec.name, "scc_size": scc.len()}),
                ));
                let kind = classify(&steps);
                found.push(cycle_anomaly(a, &kind, &steps));
                break;
            }
        }
    }

    // 3. A concrete cycle makes the matching existence result redundant.
    let types: Vec<&str> = found.iter().map(|f| f.kind.as_str()).collect();
    let impossible = model::impossible_models(&types);
    exists.retain(|e| !impossible.contains(e.detail["not"].as_str().unwrap_or("")));
    for an in exists.into_iter().chain(found) {
        a.push(an);
    }
}

/// SCCs of edges carrying any of `types`, plus composite edges
/// `a -types-> b -rw-> c`. Membership (not edge type) decides, as in Elle: an
/// edge that is both `rw` and `realtime` is still real-time evidence. A
/// composite edge can close on itself (`a -> b -rw-> a`), which counts.
fn extension_sccs(g: &Graph, scc: &[u32], types: u8) -> Vec<Vec<u32>> {
    let inside: HashMap<u32, u32> = scc.iter().enumerate().map(|(i, &v)| (v, i as u32)).collect();
    let mut b = GraphBuilder::default();
    let mut self_loops = Vec::new();
    for (i, &v) in scc.iter().enumerate() {
        for &(w, r) in &g.adj[v as usize] {
            let Some(&wl) = inside.get(&w) else { continue };
            if r & types == 0 {
                continue;
            }
            b.link(i as u32, wl, WW);
            for &(x, r2) in &g.adj[w as usize] {
                if r2 & RW != 0 {
                    if x == v {
                        self_loops.push(v);
                    } else if let Some(&xl) = inside.get(&x) {
                        b.link(i as u32, xl, WW);
                    }
                }
            }
        }
    }
    let local = b.finish(scc.len());
    let all: Vec<u32> = (0..scc.len() as u32).collect();
    let mut out: Vec<Vec<u32>> =
        local.sccs(&all, |_| true).into_iter().map(|c| c.into_iter().map(|i| scc[i as usize]).collect()).collect();
    for v in self_loops {
        if !out.iter().any(|c| c.contains(&v)) {
            out.push(vec![v]);
        }
    }
    out.sort_by_key(Vec::len);
    out
}

fn cycle_anomaly(a: &Analysis, kind: &str, steps: &[Step]) -> Anomaly {
    let ex = a.explainer.as_deref();
    let pos: Vec<usize> = steps.iter().map(|s| a.op_of[s.from as usize]).collect();
    let mut text = String::from("Let:\n");
    for &p in &pos {
        let _ = writeln!(text, "  {} = {}", a.name(p), op_json(a.h, p));
    }
    text.push_str("Then:\n");
    let mut detail = Vec::new();
    for (i, s) in steps.iter().enumerate() {
        let (from, to) = (pos[i], pos[(i + 1) % pos.len()]);
        let why = match (s.rel, ex) {
            (PROCESS | REALTIME, _) => a.explain_order(from, to, s.rel),
            (_, Some(e)) => e.explain(a, from, to, s.rel),
            (_, None) => format!("{} {} {}", a.name(from), rel_name(s.rel), a.name(to)),
        };
        let last = i + 1 == steps.len();
        let _ = writeln!(
            text,
            "  - {}{} < {}, because {}{}",
            if last { "However, " } else { "" },
            a.name(from),
            a.name(to),
            why,
            if last { ": a contradiction!" } else { "." }
        );
        detail.push(json!({"from": a.h.ops[from].index, "to": a.h.ops[to].index, "rel": rel_name(s.rel), "why": why}));
    }
    Anomaly::new(kind, pos.iter().map(|&p| a.h.ops[p].index).collect(), text, json!({ "steps": detail }))
}

/// Links each completed op to ops invoked after it completed. Like Elle, this
/// keeps only a transitive reduction: a frontier of completions not already
/// implied by a later one.
fn realtime_edges(h: &History, a: &mut Analysis, g: &mut GraphBuilder) {
    let mut frontier: Vec<usize> = Vec::new();
    let mut preceded_by: HashMap<usize, Vec<usize>> = HashMap::new();
    for (i, op) in h.ops.iter().enumerate() {
        match op.kind {
            OpType::Invoke => {
                let Some(c) = h.pair[i] else { continue };
                if matches!(h.ops[c].kind, OpType::Ok | OpType::Info) {
                    for &f in &frontier {
                        let (fv, cv) = (a.vertex(f), a.vertex(c));
                        g.link(fv, cv, REALTIME);
                    }
                    preceded_by.insert(c, frontier.clone());
                }
            }
            OpType::Ok => {
                if let Some(implied) = preceded_by.remove(&i) {
                    frontier.retain(|f| !implied.contains(f));
                }
                frontier.push(i);
            }
            _ => {}
        }
    }
}

fn process_edges(h: &History, a: &mut Analysis, g: &mut GraphBuilder) {
    let mut last: HashMap<i64, usize> = HashMap::new();
    for (i, op) in h.ops.iter().enumerate() {
        if op.kind != OpType::Ok {
            continue;
        }
        let Some(p) = op.process else { continue };
        if let Some(prev) = last.insert(p, i) {
            let (pv, iv) = (a.vertex(prev), a.vertex(i));
            g.link(pv, iv, PROCESS);
        }
    }
}

fn report(mut all: BTreeMap<String, Vec<Anomaly>>, prohibited: &BTreeSet<String>, reportable: &BTreeSet<String>) -> Report {
    let types: Vec<&str> = all.keys().map(String::as_str).collect();
    let (not, also_not) = model::boundary(&types);
    // Existence results are redundant next to anything that already rules
    // out the same model.
    let concrete: Vec<&str> =
        all.keys().filter(|t| !t.ends_with("-cycle-exists")).map(String::as_str).collect();
    let impossible = model::impossible_models(&concrete);
    let redundant: Vec<String> = all
        .keys()
        .filter(|t| t.ends_with("-cycle-exists"))
        .filter(|t| {
            CYCLE_EXISTS.iter().any(|c| format!("{}-cycle-exists", c.0) == **t && impossible.contains(c.1))
        })
        .cloned()
        .collect();
    let mut shown = BTreeMap::new();
    for (k, v) in std::mem::take(&mut all) {
        if reportable.contains(&k) && !redundant.contains(&k) {
            shown.insert(k, v);
        }
    }
    let valid = if shown.is_empty() {
        Valid::True
    } else if shown.keys().any(|k| prohibited.contains(k)) {
        Valid::False
    } else {
        Valid::Unknown
    };
    if valid == Valid::True {
        return Report { valid, anomaly_types: vec![], anomalies: shown, not: vec![], also_not: vec![] };
    }
    Report {
        valid,
        anomaly_types: shown.keys().cloned().collect(),
        anomalies: shown,
        not: not.into_iter().collect(),
        also_not: also_not.into_iter().collect(),
    }
}
