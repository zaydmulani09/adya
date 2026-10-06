//! The rw-register workload: transactions read registers and blindly
//! overwrite them with unique values.
//!
//! Registers hide their history, so the version order is only partially
//! knowable. We always know the initial state (`nil`) precedes every value;
//! optionally (`wfr_keys`) we assume a transaction that reads `x = 1` and then
//! writes `x = 2` installed 2 after 1. Each key's version graph then yields
//! `ww` and `rw` edges; `wr` edges come straight from unique writes.

use std::collections::{BTreeMap, HashMap, HashSet};

use serde_json::json;

use crate::check::{Analysis, Anomaly, Explain};
use crate::graph::{Graph, GraphBuilder, PROCESS, REALTIME, RW, WR, WW};
use crate::history::{History, Id, Mop, OpType, ReadValue};

/// A register value; `None` is the initial `nil`.
type Val = Option<Id>;

/// A transaction's external reads and writes, by key.
type Externals = (BTreeMap<Id, Val>, BTreeMap<Id, Id>);

fn read_val(v: &ReadValue) -> Option<Val> {
    match v {
        ReadValue::Nil => Some(None),
        ReadValue::Scalar(x) => Some(Some(*x)),
        ReadValue::List(_) => None,
    }
}

/// External reads (first read of a key, before writing it) and external
/// writes (last write of each key) of a transaction.
fn externals(mops: &[Mop]) -> Externals {
    let mut reads = BTreeMap::new();
    let mut writes = BTreeMap::new();
    for m in mops {
        match m {
            Mop::Read { key, value } => {
                if !writes.contains_key(key) && !reads.contains_key(key) {
                    if let Some(v) = read_val(value) {
                        reads.insert(*key, v);
                    }
                }
            }
            Mop::Write { key, value } => {
                writes.insert(*key, *value);
            }
            Mop::Append { .. } => {}
        }
    }
    (reads, writes)
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Options {
    /// Assume writes follow reads within a transaction.
    pub wfr_keys: bool,
}

pub fn analyze(h: &History) -> Analysis<'_> {
    analyze_with(h, Options::default())
}

pub fn analyze_with(h: &History, opts: Options) -> Analysis<'_> {
    let mut a = Analysis::new(h);
    for op in &h.ops {
        for m in &op.value {
            if matches!(m, Mop::Append { .. } | Mop::Read { value: ReadValue::List(_), .. }) {
                a.unknown(
                    "unexpected-txn-micro-op-types",
                    "rw-register histories may only contain writes and scalar reads",
                );
                return a;
            }
        }
    }
    let oks: Vec<usize> = (0..h.ops.len()).filter(|&i| h.ops[i].kind == OpType::Ok).collect();
    let ext: HashMap<usize, Externals> = (0..h.ops.len())
        .filter(|&i| h.ops[i].kind != OpType::Invoke)
        .map(|i| (i, externals(&h.ops[i].value)))
        .collect();

    internal(h, &oks, &mut a);
    g1a_g1b(h, &oks, &mut a);
    lost_update(h, &oks, &mut a);

    // Who externally wrote / read each version (ok ops only, as in Elle).
    let mut writers: HashMap<(Id, Id), Vec<usize>> = HashMap::new();
    let mut readers: HashMap<(Id, Val), Vec<usize>> = HashMap::new();
    for &i in &oks {
        let (r, w) = &ext[&i];
        for (k, v) in w {
            writers.entry((*k, *v)).or_default().push(i);
        }
        for (k, v) in r {
            readers.entry((*k, *v)).or_default().push(i);
        }
    }
    if let Some(((k, v), ws)) = writers.iter().find(|(_, ws)| ws.len() > 1) {
        a.unknown(
            "duplicate-writes",
            format!(
                "value {} was written to key {} by {} transactions; rw-register needs unique writes",
                h.show(*v),
                h.show(*k),
                ws.len()
            ),
        );
        return a;
    }

    // Version graphs, one source at a time; a source that would make some
    // key's versions cyclic is reported and left out.
    let mut versions: BTreeMap<Id, HashSet<(Val, Val)>> = BTreeMap::new();
    let mut initial: BTreeMap<Id, HashSet<(Val, Val)>> = BTreeMap::new();
    for (&i, (r, w)) in &ext {
        let kind = h.ops[i].kind;
        if kind == OpType::Fail {
            continue;
        }
        for (k, v) in w {
            initial.entry(*k).or_default().insert((None, Some(*v)));
        }
        if kind == OpType::Ok {
            for (k, v) in r {
                if v.is_some() {
                    initial.entry(*k).or_default().insert((None, *v));
                }
            }
        }
    }
    add_source(h, &mut a, &mut versions, initial, "initial-state");
    if opts.wfr_keys {
        let mut wfr: BTreeMap<Id, HashSet<(Val, Val)>> = BTreeMap::new();
        for &i in &oks {
            let (r, w) = &ext[&i];
            for (k, v) in r {
                if let (Some(_), Some(v2)) = (v, w.get(k)) {
                    wfr.entry(*k).or_default().insert((*v, Some(*v2)));
                }
            }
        }
        add_source(h, &mut a, &mut versions, wfr, "wfr-keys");
    }

    let mut g = GraphBuilder::default();
    for &i in &oks {
        let b = a.vertex(i);
        for (k, v) in &ext[&i].0 {
            if let Some(v) = v {
                if let Some(ws) = writers.get(&(*k, *v)) {
                    if ws[0] != i {
                        g.link(a.vertex(ws[0]), b, WR);
                    }
                }
            }
        }
    }
    for (k, edges) in &versions {
        for &(v1, v2) in edges {
            let Some(v2) = v2 else { continue };
            let to = writers.get(&(*k, v2)).map(Vec::as_slice).unwrap_or(&[]);
            let from_w = v1.and_then(|v1| writers.get(&(*k, v1))).map(Vec::as_slice).unwrap_or(&[]);
            let from_r = readers.get(&(*k, v1)).map(Vec::as_slice).unwrap_or(&[]);
            for &t in to {
                for &f in from_w {
                    if f != t {
                        let (fv, tv) = (a.vertex(f), a.vertex(t));
                        g.link(fv, tv, WW);
                    }
                }
                for &f in from_r {
                    if f != t {
                        let (fv, tv) = (a.vertex(f), a.vertex(t));
                        g.link(fv, tv, RW);
                    }
                }
            }
        }
    }
    a.data = Some(g);
    a.explainer = Some(Box::new(Explainer { versions, ext }));
    a
}

fn add_source(
    h: &History,
    a: &mut Analysis,
    versions: &mut BTreeMap<Id, HashSet<(Val, Val)>>,
    source: BTreeMap<Id, HashSet<(Val, Val)>>,
    name: &str,
) {
    let mut merged = versions.clone();
    for (k, es) in source {
        merged.entry(k).or_default().extend(es);
    }
    let mut cyclic = false;
    for (k, es) in &merged {
        // Version values -> local ids, then look for a nontrivial SCC.
        let mut ids: HashMap<Val, u32> = HashMap::new();
        let mut vals: Vec<Val> = Vec::new();
        let mut b = GraphBuilder::default();
        for &(x, y) in es {
            let mut id = |v: Val| {
                *ids.entry(v).or_insert_with(|| {
                    vals.push(v);
                    vals.len() as u32 - 1
                })
            };
            let (xi, yi) = (id(x), id(y));
            b.link(xi, yi, WW);
        }
        let g: Graph = b.finish(vals.len());
        let all: Vec<u32> = (0..vals.len() as u32).collect();
        for scc in g.sccs(&all, |_| true) {
            cyclic = true;
            let shown: Vec<String> =
                scc.iter().map(|&i| vals[i as usize].map_or("nil".into(), |v| h.show(v).to_string())).collect();
            a.push(Anomaly::new(
                "cyclic-versions",
                vec![],
                format!("assuming {name}, key {} has a cyclic version order among {}", h.show(*k), shown.join(", ")),
                json!({"key": a.scalar(*k), "source": name, "scc": shown}),
            ));
        }
    }
    if !cyclic {
        *versions = merged;
    }
}

fn internal(h: &History, oks: &[usize], a: &mut Analysis) {
    for &i in oks {
        let mut state: HashMap<Id, Val> = HashMap::new();
        for m in &h.ops[i].value {
            match m {
                Mop::Write { key, value } => {
                    state.insert(*key, Some(*value));
                }
                Mop::Read { key, value } => {
                    let Some(v) = read_val(value) else { continue };
                    if let Some(s) = state.get(key) {
                        if *s != v {
                            let show = |x: Val| x.map_or(json!(null), |x| a.scalar(x));
                            a.push(Anomaly::new(
                                "internal",
                                vec![h.ops[i].index],
                                format!(
                                    "{} read key {} as {}, but its own earlier read or write said {}",
                                    a.name(i),
                                    h.show(*key),
                                    show(v),
                                    show(*s)
                                ),
                                json!({"op": a.op_json(i), "mop": crate::check::mop_json(h, m), "expected": show(*s)}),
                            ));
                            break;
                        }
                    }
                    state.insert(*key, v);
                }
                Mop::Append { .. } => {}
            }
        }
    }
}

fn g1a_g1b(h: &History, oks: &[usize], a: &mut Analysis) {
    let mut failed: HashMap<(Id, Id), usize> = HashMap::new();
    let mut intermediate: HashMap<(Id, Id), usize> = HashMap::new();
    for (i, op) in h.ops.iter().enumerate() {
        if op.kind == OpType::Invoke {
            continue;
        }
        let mut last: HashMap<Id, Id> = HashMap::new();
        for m in &op.value {
            if let Mop::Write { key, value } = m {
                if op.kind == OpType::Fail {
                    failed.insert((*key, *value), i);
                }
                if let Some(prev) = last.insert(*key, *value) {
                    intermediate.insert((*key, prev), i);
                }
            }
        }
    }
    for &i in oks {
        for m in &h.ops[i].value {
            let Mop::Read { key, value: ReadValue::Scalar(v) } = m else { continue };
            if let Some(&w) = failed.get(&(*key, *v)) {
                a.push(Anomaly::new(
                    "G1a",
                    vec![h.ops[i].index, h.ops[w].index],
                    format!(
                        "{} read {} = {}, written by {}, which failed (aborted read)",
                        a.name(i),
                        h.show(*key),
                        h.show(*v),
                        a.name(w)
                    ),
                    json!({"op": a.op_json(i), "mop": crate::check::mop_json(h, m), "writer": a.op_json(w)}),
                ));
            }
            if let Some(&w) = intermediate.get(&(*key, *v)) {
                if w != i {
                    a.push(Anomaly::new(
                        "G1b",
                        vec![h.ops[i].index, h.ops[w].index],
                        format!(
                            "{} read {} = {}, which {} overwrote before committing (intermediate read)",
                            a.name(i),
                            h.show(*key),
                            h.show(*v),
                            a.name(w)
                        ),
                        json!({"op": a.op_json(i), "mop": crate::check::mop_json(h, m), "writer": a.op_json(w)}),
                    ));
                }
            }
        }
    }
}

fn lost_update(h: &History, oks: &[usize], a: &mut Analysis) {
    let mut groups: BTreeMap<(Id, Val), Vec<usize>> = BTreeMap::new();
    for &i in oks {
        let mut reads: HashMap<Id, Val> = HashMap::new();
        let mut writes: HashSet<Id> = HashSet::new();
        for m in &h.ops[i].value {
            match m {
                Mop::Write { key, .. } if !writes.contains(key) => {
                    if let Some(v) = reads.get(key) {
                        groups.entry((*key, *v)).or_default().push(i);
                        writes.insert(*key);
                    }
                }
                Mop::Read { key, value } => {
                    if let Some(v) = read_val(value) {
                        reads.entry(*key).or_insert(v);
                    }
                }
                _ => {}
            }
        }
    }
    for ((k, v), txns) in groups {
        if txns.len() >= 2 {
            let shown = v.map_or(json!(null), |v| a.scalar(v));
            a.push(Anomaly::new(
                "lost-update",
                txns.iter().map(|&t| h.ops[t].index).collect(),
                format!(
                    "{} transactions ({}) all read key {} = {} and then wrote it; all but one of those writes was based on a stale read (lost update)",
                    txns.len(),
                    txns.iter().map(|&t| a.name(t)).collect::<Vec<_>>().join(", "),
                    h.show(k),
                    shown
                ),
                json!({"key": a.scalar(k), "value": shown, "txns": txns.iter().map(|&t| a.op_json(t)).collect::<Vec<_>>()}),
            ));
        }
    }
}

struct Explainer {
    versions: BTreeMap<Id, HashSet<(Val, Val)>>,
    ext: HashMap<usize, Externals>,
}

impl Explain for Explainer {
    fn explain(&self, a: &Analysis, from: usize, to: usize, rel: u8) -> String {
        let h = a.history();
        let (an, bn) = (a.name(from), a.name(to));
        let (ra, wa) = &self.ext[&from];
        let (rb, wb) = &self.ext[&to];
        let show = |v: Val| v.map_or("nil".to_string(), |v| h.show(v).to_string());
        let later = |k: &Id, v1: Val, v2: Id| self.versions.get(k).is_some_and(|es| es.contains(&(v1, Some(v2))));
        match rel {
            WR => {
                for (k, v) in wa {
                    if rb.get(k) == Some(&Some(*v)) {
                        return format!("{an} wrote {} = {}, which was read by {bn}", h.show(*k), h.show(*v));
                    }
                }
            }
            WW => {
                for (k, v) in wa {
                    if let Some(v2) = wb.get(k) {
                        if later(k, Some(*v), *v2) {
                            return format!(
                                "{an} set key {} to {}, and {bn} set it to {}, which came later in the version order",
                                h.show(*k),
                                h.show(*v),
                                h.show(*v2)
                            );
                        }
                    }
                }
            }
            RW => {
                for (k, v) in ra {
                    if let Some(v2) = wb.get(k) {
                        if later(k, *v, *v2) {
                            return format!(
                                "{an} read key {} = {}, and {bn} set it to {}, which came later in the version order",
                                h.show(*k),
                                show(*v),
                                h.show(*v2)
                            );
                        }
                    }
                }
            }
            PROCESS | REALTIME => return a.explain_order(from, to, rel),
            _ => {}
        }
        format!("{an} precedes {bn}")
    }
}
