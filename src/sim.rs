//! A deterministic in-memory database for exercising the checker.
//!
//! Processes interleave one step at a time (invoke, each micro-op, commit),
//! chosen by a seeded scheduler, against a multiversion store that enforces
//! one of several isolation levels. Because each level is implemented the
//! textbook way, the histories it produces have known properties: a
//! serializable run must check clean under strict serializability, a snapshot
//! run should show write skew but nothing SI forbids, and so on. `adya run sim`
//! uses it as a zero-setup demo; the test suite uses it to look for false
//! positives and missed anomalies at scale.

use std::collections::{BTreeMap, HashMap};

use crate::gen::{op_line, Gen, GenOpts, Rng, TxnOp};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Isolation {
    /// Each transaction executes atomically when it commits.
    Serializable,
    /// Reads from a snapshot taken at start; first committer wins.
    SnapshotIsolation,
    /// Reads see the latest committed state; writes take a lock and apply at
    /// commit.
    ReadCommitted,
    /// Writes take a lock and apply immediately, visible to everyone; aborts
    /// roll back.
    ReadUncommitted,
    /// A broken snapshot implementation: no write-conflict check, and commit
    /// writes back the transaction's stale snapshot plus its own appends.
    LostUpdate,
}

impl Isolation {
    pub fn parse(s: &str) -> Option<Isolation> {
        Some(match s {
            "serializable" => Isolation::Serializable,
            "snapshot-isolation" | "si" => Isolation::SnapshotIsolation,
            "read-committed" | "rc" => Isolation::ReadCommitted,
            "read-uncommitted" | "ru" => Isolation::ReadUncommitted,
            "lost-update" => Isolation::LostUpdate,
            _ => return None,
        })
    }
}

#[derive(Clone, Debug)]
pub struct SimOpts {
    pub isolation: Isolation,
    pub processes: usize,
    pub txns: usize,
    /// Probability a transaction aborts at commit for no reason.
    pub abort_rate: f64,
    /// Probability a commit's outcome is reported as unknown (`info`).
    pub info_rate: f64,
    pub gen: GenOpts,
    pub seed: u64,
}

impl Default for SimOpts {
    fn default() -> SimOpts {
        SimOpts {
            isolation: Isolation::Serializable,
            processes: 5,
            txns: 1000,
            abort_rate: 0.05,
            info_rate: 0.01,
            gen: GenOpts::default(),
            seed: 0,
        }
    }
}

/// A committed version: lists hold every element; registers hold one.
type State = Vec<i64>;

struct Txn {
    ops: Vec<TxnOp>,
    pos: usize,
    start: u64,
    /// Results so far: reads filled in as they execute.
    reads: Vec<Option<State>>,
    /// Buffered writes: key -> elements appended (or the last value written).
    writes: BTreeMap<i64, Vec<i64>>,
    /// Snapshot of each key as read, for the lost-update bug.
    seen: HashMap<i64, State>,
    /// Lost a write lock (no-wait locking); will abort at commit.
    doomed: bool,
}

struct Store {
    /// key -> (commit timestamp, state), oldest first.
    versions: HashMap<i64, Vec<(u64, State)>>,
    clock: u64,
    /// Read-uncommitted: the live, dirty state.
    dirty: HashMap<i64, State>,
    /// Write locks (read committed / uncommitted): key -> holding process.
    locks: HashMap<i64, usize>,
}

impl Store {
    fn at(&self, k: i64, ts: u64) -> State {
        self.versions.get(&k).and_then(|vs| vs.iter().rev().find(|(t, _)| *t <= ts)).map(|(_, s)| s.clone()).unwrap_or_default()
    }

    fn latest(&self, k: i64) -> State {
        self.at(k, u64::MAX)
    }

    fn last_commit(&self, k: i64) -> u64 {
        self.versions.get(&k).and_then(|vs| vs.last()).map_or(0, |(t, _)| *t)
    }

    fn install(&mut self, k: i64, s: State) {
        self.versions.entry(k).or_default().push((self.clock, s));
    }
}

fn apply(state: &mut State, op: TxnOp) {
    match op {
        TxnOp::Append(_, v) => state.push(v),
        TxnOp::Write(_, v) => *state = vec![v],
        TxnOp::Read(_) => {}
    }
}

/// Runs a simulated workload and returns the history as JSON lines.
pub fn run(opts: &SimOpts) -> String {
    let mut rng = Rng::new(opts.seed);
    let mut gen = Gen::new(opts.gen.clone(), opts.seed.wrapping_add(1));
    let mut store = Store { versions: HashMap::new(), clock: 0, dirty: HashMap::new(), locks: HashMap::new() };
    let mut procs: Vec<Option<Txn>> = (0..opts.processes).map(|_| None).collect();
    // Process ids change after an info, like Jepsen: the old one may still
    // be running for all we know.
    let mut pid: Vec<usize> = (0..opts.processes).collect();
    let mut out = String::new();
    let mut index = 0usize;
    let mut started = 0usize;
    let mut tick = 0u64;
    let list = opts.gen.kind == crate::gen::Kind::ListAppend;

    while started < opts.txns || procs.iter().any(Option::is_some) {
        tick += 1;
        let p = rng.below(opts.processes as u64) as usize;
        let Some(t) = procs[p].as_mut() else {
            if started < opts.txns {
                started += 1;
                let ops = gen.txn();
                out.push_str(&op_line(index, "invoke", pid[p], tick * 1000, &ops, &vec![None; ops.len()], list));
                procs[p] = Some(Txn {
                    reads: vec![None; ops.len()],
                    ops,
                    pos: 0,
                    start: store.clock,
                    writes: BTreeMap::new(),
                    seen: HashMap::new(),
                    doomed: false,
                });
                index += 1;
            }
            continue;
        };

        if t.pos < t.ops.len() && opts.isolation != Isolation::Serializable {
            step(&mut store, t, opts.isolation, p);
            continue;
        }

        // Commit.
        let mut t = procs[p].take().unwrap();
        let abort = rng.chance(opts.abort_rate) || t.doomed;
        store.locks.retain(|_, holder| *holder != p);
        let ok = match opts.isolation {
            Isolation::Serializable => {
                if !abort {
                    let mut local: HashMap<i64, State> = HashMap::new();
                    for (i, op) in t.ops.iter().enumerate() {
                        let k = key(*op);
                        let s = local.entry(k).or_insert_with(|| store.latest(k));
                        if let TxnOp::Read(_) = op {
                            t.reads[i] = Some(s.clone());
                        }
                        apply(s, *op);
                    }
                    store.clock += 1;
                    for (k, s) in local {
                        if t.ops.iter().any(|o| key(*o) == k && !matches!(o, TxnOp::Read(_))) {
                            store.install(k, s);
                        }
                    }
                }
                !abort
            }
            Isolation::SnapshotIsolation => {
                let conflict = t.writes.keys().any(|k| store.last_commit(*k) > t.start);
                if abort || conflict {
                    false
                } else {
                    commit_writes(&mut store, &t, false);
                    true
                }
            }
            Isolation::ReadCommitted => {
                if !abort {
                    commit_writes(&mut store, &t, false);
                }
                !abort
            }
            Isolation::LostUpdate => {
                if !abort {
                    commit_writes(&mut store, &t, true);
                }
                !abort
            }
            Isolation::ReadUncommitted => {
                if abort {
                    // Roll back: remove this transaction's elements.
                    for (k, vs) in &t.writes {
                        if let Some(s) = store.dirty.get_mut(k) {
                            s.retain(|e| !vs.contains(e));
                        }
                    }
                } else {
                    store.clock += 1;
                    for k in t.writes.keys() {
                        let s = store.dirty.get(k).cloned().unwrap_or_default();
                        store.install(*k, s);
                    }
                }
                !abort
            }
        };
        let kind = if ok && rng.chance(opts.info_rate) {
            "info"
        } else if ok {
            "ok"
        } else {
            "fail"
        };
        let reads = if kind == "ok" { t.reads.clone() } else { vec![None; t.ops.len()] };
        out.push_str(&op_line(index, kind, pid[p], tick * 1000, &t.ops, &reads, list));
        index += 1;
        if kind == "info" {
            pid[p] += opts.processes;
        }
    }
    out
}

fn key(op: TxnOp) -> i64 {
    match op {
        TxnOp::Append(k, _) | TxnOp::Write(k, _) | TxnOp::Read(k) => k,
    }
}

/// Executes the next micro-op of a non-serializable transaction.
fn step(store: &mut Store, t: &mut Txn, iso: Isolation, p: usize) {
    let op = t.ops[t.pos];
    let k = key(op);
    let locking = matches!(iso, Isolation::ReadCommitted | Isolation::ReadUncommitted);
    if t.doomed {
        t.pos += 1;
        return;
    }
    if locking && !matches!(op, TxnOp::Read(_)) && *store.locks.entry(k).or_insert(p) != p {
        // No-wait: someone else holds the write lock, so give up.
        t.doomed = true;
        t.pos += 1;
        return;
    }
    match op {
        TxnOp::Read(_) => {
            let mut s = match iso {
                Isolation::SnapshotIsolation | Isolation::LostUpdate => store.at(k, t.start),
                Isolation::ReadCommitted => store.latest(k),
                Isolation::ReadUncommitted => store.dirty.get(&k).cloned().unwrap_or_default(),
                Isolation::Serializable => unreachable!(),
            };
            t.seen.entry(k).or_insert_with(|| s.clone());
            if iso != Isolation::ReadUncommitted {
                // Our own earlier writes are visible to us.
                if let Some(w) = t.writes.get(&k) {
                    if t.ops.iter().any(|o| matches!(o, TxnOp::Write(..))) {
                        s = vec![*w.last().unwrap()];
                    } else {
                        s.extend(w);
                    }
                }
            }
            t.reads[t.pos] = Some(s);
        }
        TxnOp::Append(_, v) | TxnOp::Write(_, v) => {
            if iso == Isolation::LostUpdate {
                t.seen.entry(k).or_insert_with(|| store.at(k, t.start));
            }
            let w = t.writes.entry(k).or_default();
            if matches!(op, TxnOp::Write(..)) {
                w.clear();
            }
            w.push(v);
            if iso == Isolation::ReadUncommitted {
                apply(store.dirty.entry(k).or_default(), op);
            }
        }
    }
    t.pos += 1;
}

fn commit_writes(store: &mut Store, t: &Txn, stale: bool) {
    store.clock += 1;
    for (k, vs) in &t.writes {
        let register = t.ops.iter().any(|o| matches!(o, TxnOp::Write(kk, _) if kk == k));
        let mut s = if stale { t.seen.get(k).cloned().unwrap_or_default() } else { store.latest(*k) };
        if register {
            s = vec![*vs.last().unwrap()];
        } else {
            s.extend(vs);
        }
        store.install(*k, s);
    }
}
