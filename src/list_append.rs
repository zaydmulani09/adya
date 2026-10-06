//! The list-append workload: transactions append unique elements to lists
//! and read whole lists back.
//!
//! Every read reveals the order of every append before it, so the version
//! order of each key falls out of the longest read, and with it Adya's direct
//! dependencies: `ww` between successive appends, `wr` from an append to reads
//! that end with it, and `rw` from reads to the append that came next.

use std::collections::{BTreeMap, HashMap, HashSet};

use serde_json::json;

use crate::check::{Analysis, Anomaly, Explain};
use crate::graph::{GraphBuilder, PROCESS, REALTIME, RW, WR, WW};
use crate::history::{History, Id, Mop, OpType, ReadValue};

/// A read's list, treating `nil` on a completion as the empty list.
fn read_list(v: &ReadValue) -> Option<&[Id]> {
    match v {
        ReadValue::List(xs) => Some(xs),
        ReadValue::Nil => Some(&[]),
        ReadValue::Scalar(_) => None,
    }
}

/// Element appended before `v` on its key: `Some(Some(e))`, or `Some(None)`
/// for the initial state.
type Prev = Option<Id>;

struct Index {
    /// key -> version order (distinct elements of the longest read).
    order: HashMap<Id, Vec<Id>>,
    /// key -> element -> position in `order`.
    position: HashMap<Id, HashMap<Id, usize>>,
    /// (key, element) -> completion op position (ok + info only).
    writer: HashMap<(Id, Id), usize>,
    /// (key, last element or None for the initial state) -> readers.
    readers: HashMap<(Id, Prev), Vec<usize>>,
}

impl Index {
    fn prev(&self, key: Id, v: Id) -> Prev {
        let order = self.order.get(&key).map(Vec::as_slice).unwrap_or(&[]);
        match self.position.get(&key).and_then(|p| p.get(&v)) {
            Some(&0) => None,
            Some(&i) => Some(order[i - 1]),
            // Never observed: it still came after everything we did observe.
            None => order.last().copied(),
        }
    }
}

pub fn analyze(h: &History) -> Analysis<'_> {
    let mut a = Analysis::new(h);

    // Well-formedness: only appends and list reads, and unique appends.
    let mut seen = HashSet::new();
    for op in &h.ops {
        for m in &op.value {
            match m {
                Mop::Append { key, value } if op.kind == OpType::Invoke && !seen.insert((*key, *value)) => {
                    a.unknown(
                        "duplicate-appends",
                        format!("element {} was appended to key {} more than once", h.show(*value), h.show(*key)),
                    );
                    return a;
                }
                Mop::Write { .. } | Mop::Read { value: ReadValue::Scalar(_), .. } => {
                    a.unknown("unexpected-txn-micro-op-types", "list-append histories may only contain appends and list reads");
                    return a;
                }
                _ => {}
            }
        }
    }

    let completions: Vec<usize> = (0..h.ops.len()).filter(|&i| h.ops[i].kind != OpType::Invoke).collect();
    let possible: Vec<usize> = completions.iter().copied().filter(|&i| matches!(h.ops[i].kind, OpType::Ok | OpType::Info)).collect();
    let oks: Vec<usize> = completions.iter().copied().filter(|&i| h.ops[i].kind == OpType::Ok).collect();

    g1a(h, &oks, &mut a);
    g1b(h, &oks, &mut a);
    internal(h, &oks, &mut a);
    duplicates(h, &possible, &mut a);
    let order = version_orders(h, &possible, &mut a);
    dirty_update(h, &completions, &order, &mut a);
    lost_update(h, &oks, &mut a);

    // Index and graph over possibly-committed transactions.
    let mut idx = Index { position: HashMap::new(), writer: HashMap::new(), readers: HashMap::new(), order };
    for (k, vs) in &idx.order {
        idx.position.insert(*k, vs.iter().enumerate().map(|(i, v)| (*v, i)).collect());
    }
    for &i in &possible {
        let op = &h.ops[i];
        for m in &op.value {
            match m {
                Mop::Append { key, value } => {
                    idx.writer.insert((*key, *value), i);
                }
                Mop::Read { key, value } => {
                    // An info op's nil read means "never happened", not "empty".
                    if op.kind == OpType::Info && *value == ReadValue::Nil {
                        continue;
                    }
                    let last = read_list(value).and_then(|xs| xs.last().copied());
                    idx.readers.entry((*key, last)).or_default().push(i);
                }
                Mop::Write { .. } => {}
            }
        }
    }

    let mut g = GraphBuilder::default();
    for &i in &possible {
        let b = a.vertex(i);
        for m in &h.ops[i].value {
            match m {
                Mop::Read { key, value } => {
                    if let Some(&last) = read_list(value).and_then(|xs| xs.last()) {
                        if let Some(&w) = idx.writer.get(&(*key, last)) {
                            if w != i {
                                g.link(a.vertex(w), b, WR);
                            }
                        }
                    }
                }
                Mop::Append { key, value } => {
                    let prev = idx.prev(*key, *value);
                    if let Some(p) = prev {
                        if let Some(&w) = idx.writer.get(&(*key, p)) {
                            if w != i {
                                g.link(a.vertex(w), b, WW);
                            }
                        }
                    }
                    for &r in idx.readers.get(&(*key, prev)).map(Vec::as_slice).unwrap_or(&[]) {
                        if r != i {
                            g.link(a.vertex(r), b, RW);
                        }
                    }
                }
                Mop::Write { .. } => {}
            }
        }
    }
    a.data = Some(g);
    a.explainer = Some(Box::new(Explainer { idx }));
    a
}

fn mop_json(h: &History, m: &Mop) -> serde_json::Value {
    crate::check::mop_json(h, m)
}

fn g1a(h: &History, oks: &[usize], a: &mut Analysis) {
    let mut failed: HashMap<(Id, Id), usize> = HashMap::new();
    for (i, op) in h.ops.iter().enumerate() {
        if op.kind == OpType::Fail {
            for m in &op.value {
                if let Mop::Append { key, value } = m {
                    failed.insert((*key, *value), i);
                }
            }
        }
    }
    for &i in oks {
        for m in &h.ops[i].value {
            let Mop::Read { key, value } = m else { continue };
            for &e in read_list(value).unwrap_or(&[]) {
                if let Some(&w) = failed.get(&(*key, e)) {
                    a.push(Anomaly::new(
                        "G1a",
                        vec![h.ops[i].index, h.ops[w].index],
                        format!(
                            "{} read element {} of key {}, but that element was appended by {}, which failed (aborted read)",
                            a.name(i),
                            h.show(e),
                            h.show(*key),
                            a.name(w)
                        ),
                        json!({"op": a.op_json(i), "mop": mop_json(h, m), "writer": a.op_json(w), "element": a.scalar(e)}),
                    ));
                }
            }
        }
    }
}

fn g1b(h: &History, oks: &[usize], a: &mut Analysis) {
    // (key, element) -> op that appended it but appended to that key again later.
    let mut intermediate: HashMap<(Id, Id), usize> = HashMap::new();
    for (i, op) in h.ops.iter().enumerate() {
        if op.kind == OpType::Invoke {
            continue;
        }
        let mut last: HashMap<Id, Id> = HashMap::new();
        for m in &op.value {
            if let Mop::Append { key, value } = m {
                if let Some(prev) = last.insert(*key, *value) {
                    intermediate.insert((*key, prev), i);
                }
            }
        }
    }
    for &i in oks {
        for m in &h.ops[i].value {
            let Mop::Read { key, value } = m else { continue };
            let Some(&e) = read_list(value).and_then(|xs| xs.last()) else { continue };
            if let Some(&w) = intermediate.get(&(*key, e)) {
                if w != i {
                    a.push(Anomaly::new(
                        "G1b",
                        vec![h.ops[i].index, h.ops[w].index],
                        format!(
                            "{} read key {} ending in {}, an intermediate state: {} appended more to that key afterwards (intermediate read)",
                            a.name(i),
                            h.show(*key),
                            h.show(e),
                            a.name(w)
                        ),
                        json!({"op": a.op_json(i), "mop": mop_json(h, m), "writer": a.op_json(w), "element": a.scalar(e)}),
                    ));
                }
            }
        }
    }
}

/// What a transaction expects a key to hold, given its own earlier mops.
enum Expect {
    Exactly(Vec<Id>),
    /// Something unknown, followed by these elements.
    EndsWith(Vec<Id>),
}

fn internal(h: &History, oks: &[usize], a: &mut Analysis) {
    for &i in oks {
        let mut state: HashMap<Id, Expect> = HashMap::new();
        let mut appended: HashSet<(Id, Id)> = HashSet::new();
        let mut reported_future = false;
        let mut read_elems: HashMap<Id, HashMap<Id, &Mop>> = HashMap::new();
        for m in &h.ops[i].value {
            match m {
                Mop::Append { key, value } => {
                    appended.insert((*key, *value));
                    if !reported_future {
                        if let Some(rm) = read_elems.get(key).and_then(|r| r.get(value)) {
                            reported_future = true;
                            a.push(Anomaly::new(
                                "future-read",
                                vec![h.ops[i].index],
                                format!(
                                    "{} read element {} of key {} before appending it itself",
                                    a.name(i),
                                    h.show(*value),
                                    h.show(*key)
                                ),
                                json!({"op": a.op_json(i), "mop": mop_json(h, rm), "element": a.scalar(*value)}),
                            ));
                        }
                    }
                    match state.get_mut(key) {
                        Some(Expect::Exactly(v)) | Some(Expect::EndsWith(v)) => v.push(*value),
                        None => {
                            state.insert(*key, Expect::EndsWith(vec![*value]));
                        }
                    }
                }
                Mop::Read { key, value } => {
                    let Some(v) = read_list(value) else { continue };
                    for e in v {
                        read_elems.entry(*key).or_default().entry(*e).or_insert(m);
                    }
                    let bad = match state.get(key) {
                        Some(Expect::Exactly(s)) => s.as_slice() != v,
                        Some(Expect::EndsWith(s)) => s.len() > v.len() || &v[v.len() - s.len()..] != s.as_slice(),
                        None => false,
                    };
                    if bad {
                        let expected = match &state[key] {
                            Expect::Exactly(s) => json!(s.iter().map(|e| a.scalar(*e)).collect::<Vec<_>>()),
                            Expect::EndsWith(s) => {
                                let mut x = vec![json!("...")];
                                x.extend(s.iter().map(|e| a.scalar(*e)));
                                json!(x)
                            }
                        };
                        a.push(Anomaly::new(
                            "internal",
                            vec![h.ops[i].index],
                            format!(
                                "{} read key {} as {}, inconsistent with its own earlier reads and appends (expected {})",
                                a.name(i),
                                h.show(*key),
                                a.list(v),
                                expected
                            ),
                            json!({"op": a.op_json(i), "mop": mop_json(h, m), "expected": expected}),
                        ));
                        break;
                    }
                    state.insert(*key, Expect::Exactly(v.to_vec()));
                }
                Mop::Write { .. } => {}
            }
        }
    }
}

fn duplicates(h: &History, possible: &[usize], a: &mut Analysis) {
    for &i in possible {
        for m in &h.ops[i].value {
            let Mop::Read { value, .. } = m else { continue };
            let Some(v) = read_list(value) else { continue };
            let mut counts: BTreeMap<Id, usize> = BTreeMap::new();
            for e in v {
                *counts.entry(*e).or_default() += 1;
            }
            let dups: Vec<_> = counts.into_iter().filter(|(_, c)| *c > 1).collect();
            if !dups.is_empty() {
                a.push(Anomaly::new(
                    "duplicate-elements",
                    vec![h.ops[i].index],
                    format!("{} read {}, which contains duplicates even though every append is unique", a.name(i), a.list(v)),
                    json!({"op": a.op_json(i), "mop": mop_json(h, m),
                           "duplicates": dups.iter().map(|(e, c)| json!([a.scalar(*e), c])).collect::<Vec<_>>()}),
                ));
            }
        }
    }
}

/// Infers each key's version order from the longest read, and reports reads
/// that cannot both be prefixes of one history of appends.
fn version_orders(h: &History, possible: &[usize], a: &mut Analysis) -> HashMap<Id, Vec<Id>> {
    let mut values: HashMap<Id, HashSet<Vec<Id>>> = HashMap::new();
    let mut appends: HashMap<Id, Option<Id>> = HashMap::new(); // None = more than one
    for &i in possible {
        for m in &h.ops[i].value {
            match m {
                Mop::Read { key, value } => {
                    if let Some(v) = read_list(value).filter(|v| !v.is_empty()) {
                        values.entry(*key).or_default().insert(v.to_vec());
                    }
                }
                Mop::Append { key, value } => {
                    appends.entry(*key).and_modify(|x| *x = None).or_insert(Some(*value));
                }
                Mop::Write { .. } => {}
            }
        }
    }
    // A key appended exactly once needs no read to know its order.
    for (k, v) in appends {
        if let (Some(v), None) = (v, values.get(&k)) {
            values.insert(k, HashSet::from([vec![v]]));
        }
    }
    let mut keys: Vec<Id> = values.keys().copied().collect();
    keys.sort_unstable();
    let mut order = HashMap::new();
    for k in keys {
        let mut vs: Vec<Vec<Id>> = values.remove(&k).unwrap().into_iter().collect();
        vs.sort_by(|x, y| x.len().cmp(&y.len()).then_with(|| x.cmp(y)));
        if let Some(w) = vs.windows(2).find(|w| !w[1].starts_with(&w[0])) {
            a.push(Anomaly::new(
                "incompatible-order",
                vec![],
                format!(
                    "key {} was read as both {} and {}; neither is a prefix of the other, so no single order of appends explains them",
                    h.show(k),
                    a.list(&w[0]),
                    a.list(&w[1])
                ),
                json!({"key": a.scalar(k), "values": [a.list_json(&w[0]), a.list_json(&w[1])]}),
            ));
        }
        let longest = vs.pop().unwrap_or_default();
        let mut dedup = HashSet::new();
        order.insert(k, longest.into_iter().filter(|e| dedup.insert(*e)).collect());
    }
    order
}

/// An aborted transaction's append, followed by a committed one on the same
/// key: the committed write built on state that should never have existed.
fn dirty_update(h: &History, completions: &[usize], order: &HashMap<Id, Vec<Id>>, a: &mut Analysis) {
    let mut writer: HashMap<(Id, Id), usize> = HashMap::new();
    for &i in completions {
        for m in &h.ops[i].value {
            if let Mop::Append { key, value } = m {
                writer.insert((*key, *value), i);
            }
        }
    }
    let mut keys: Vec<&Id> = order.keys().collect();
    keys.sort_unstable();
    for k in keys {
        // (last relevant writer, its kind) and the elements spanning the case.
        let mut t1: Option<usize> = None; // None = initial state (committed)
        let mut span: Vec<Id> = Vec::new();
        for &v in &order[k] {
            let Some(&t2) = writer.get(&(*k, v)) else { break };
            let k1 = t1.map_or(OpType::Ok, |t| h.ops[t].kind);
            let k2 = h.ops[t2].kind;
            match (k1, k2) {
                (OpType::Fail, OpType::Ok) => {
                    span.push(v);
                    let t1i = t1.unwrap();
                    a.push(Anomaly::new(
                        "dirty-update",
                        vec![h.ops[t1i].index, h.ops[t2].index],
                        format!(
                            "on key {}, {} committed an append of {} on top of state left by {}, which failed (dirty update)",
                            h.show(*k),
                            a.name(t2),
                            h.show(v),
                            a.name(t1i)
                        ),
                        json!({"key": a.scalar(*k), "values": a.list_json(&span), "txns": [a.op_json(t1i), "...", a.op_json(t2)]}),
                    ));
                    t1 = Some(t2);
                    span = vec![v];
                }
                (_, OpType::Info) | (OpType::Fail, OpType::Fail) => span.push(v),
                _ => {
                    t1 = Some(t2);
                    span = vec![v];
                }
            }
        }
    }
}

/// Two committed transactions that read the same state of a key and both
/// appended to it: one of their updates was built on a stale read.
fn lost_update(h: &History, oks: &[usize], a: &mut Analysis) {
    let mut groups: BTreeMap<(Id, Vec<Id>, bool), Vec<usize>> = BTreeMap::new();
    for &i in oks {
        let mut reads: HashMap<Id, &ReadValue> = HashMap::new();
        let mut writes: HashSet<Id> = HashSet::new();
        for m in &h.ops[i].value {
            match m {
                Mop::Append { key, .. } if !writes.contains(key) => {
                    if let Some(r) = reads.get(key) {
                        let nil = **r == ReadValue::Nil;
                        let v = read_list(r).unwrap_or(&[]).to_vec();
                        groups.entry((*key, v, nil)).or_default().push(i);
                        writes.insert(*key);
                    }
                }
                Mop::Read { key, value } => {
                    reads.entry(*key).or_insert(value);
                }
                _ => {}
            }
        }
    }
    for ((k, v, _), txns) in groups {
        if txns.len() >= 2 {
            a.push(Anomaly::new(
                "lost-update",
                txns.iter().map(|&t| h.ops[t].index).collect(),
                format!(
                    "{} transactions ({}) all read key {} as {} and then appended to it; all but one of those updates was based on a stale read (lost update)",
                    txns.len(),
                    txns.iter().map(|&t| a.name(t)).collect::<Vec<_>>().join(", "),
                    h.show(k),
                    a.list(&v)
                ),
                json!({"key": a.scalar(k), "value": a.list_json(&v), "txns": txns.iter().map(|&t| a.op_json(t)).collect::<Vec<_>>()}),
            ));
        }
    }
}

struct Explainer {
    idx: Index,
}

impl Explain for Explainer {
    fn explain(&self, a: &Analysis, from: usize, to: usize, rel: u8) -> String {
        let h = a.history();
        let (an, bn) = (a.name(from), a.name(to));
        let key_of = |k: Id| h.show(k).to_string();
        match rel {
            WW => {
                for m in &h.ops[to].value {
                    if let Mop::Append { key, value } = m {
                        if let Some(p) = self.idx.prev(*key, *value) {
                            if self.idx.writer.get(&(*key, p)) == Some(&from) {
                                return format!("{bn} appended {} after {an} appended {} to {}", h.show(*value), h.show(p), key_of(*key));
                            }
                        }
                    }
                }
            }
            WR => {
                for m in &h.ops[to].value {
                    if let Mop::Read { key, value } = m {
                        if let Some(&last) = read_list(value).and_then(|v| v.last()) {
                            if self.idx.writer.get(&(*key, last)) == Some(&from) {
                                return format!("{bn} observed {an}'s append of {} to key {}", h.show(last), key_of(*key));
                            }
                        }
                    }
                }
            }
            RW => {
                for m in &h.ops[to].value {
                    if let Mop::Append { key, value } = m {
                        let prev = self.idx.prev(*key, *value);
                        if self.idx.readers.get(&(*key, prev)).is_some_and(|r| r.contains(&from)) {
                            return match prev {
                                None => format!(
                                    "{an} observed the initial (empty) state of {}, which {bn} created by appending {}",
                                    key_of(*key),
                                    h.show(*value)
                                ),
                                Some(_) => format!("{an} did not observe {bn}'s append of {} to {}", h.show(*value), key_of(*key)),
                            };
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
