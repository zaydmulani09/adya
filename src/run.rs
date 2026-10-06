//! Running a generated workload against a real database and recording the
//! history.
//!
//! Each logical process is a thread with its own connection. Invocations and
//! completions are appended to the history under one lock, so the history's
//! order is a valid real-time order. After an indeterminate outcome (`info`)
//! the process retires its id and reconnects, as in Jepsen: the old
//! transaction may still be in flight.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::gen::{op_line, Gen, GenOpts, Kind, TxnOp};
use crate::Error;

/// How a transaction ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Committed. `reads[i]` holds the result of `ops[i]` when it is a read:
    /// a list's elements, or zero/one element for a register.
    Ok(Vec<Option<Vec<i64>>>),
    /// Definitely did not commit.
    Fail(String),
    /// Might or might not have committed.
    Info(String),
}

/// A connection that can execute one transaction at a time.
pub trait Client: Send {
    fn txn(&mut self, ops: &[TxnOp]) -> Outcome;
}

/// Opens a fresh client for a process.
pub type Connect<'a> = dyn Fn() -> Result<Box<dyn Client>, Error> + Sync + 'a;

#[derive(Clone, Debug)]
pub struct RunOpts {
    pub processes: usize,
    pub txns: usize,
    /// Stop early after this long, if set.
    pub time_limit: Option<Duration>,
    pub gen: GenOpts,
    pub seed: u64,
}

impl Default for RunOpts {
    fn default() -> RunOpts {
        RunOpts { processes: 8, txns: 2000, time_limit: None, gen: GenOpts::default(), seed: 0 }
    }
}

/// Counts of how transactions ended.
#[derive(Debug, Default, Clone, Copy)]
pub struct Stats {
    pub ok: usize,
    pub fail: usize,
    pub info: usize,
}

struct Shared {
    gen: Gen,
    started: usize,
    index: usize,
    history: String,
    stats: Stats,
    last_error: Option<String>,
}

/// Runs the workload; returns the history as JSON lines.
pub fn run(connect: &Connect, opts: &RunOpts) -> Result<(String, Stats), Error> {
    let start = Instant::now();
    let deadline = opts.time_limit.map(|d| start + d);
    let list = opts.gen.kind == Kind::ListAppend;
    let shared = Mutex::new(Shared {
        gen: Gen::new(opts.gen.clone(), opts.seed),
        started: 0,
        index: 0,
        history: String::new(),
        stats: Stats::default(),
        last_error: None,
    });
    // Fail fast on a bad connection string rather than once per thread.
    drop(connect()?);

    std::thread::scope(|s| {
        for p in 0..opts.processes {
            let shared = &shared;
            s.spawn(move || {
                let mut pid = p;
                let mut client = None;
                loop {
                    if deadline.is_some_and(|d| Instant::now() > d) {
                        return;
                    }
                    let c = match client.as_mut() {
                        Some(c) => c,
                        None => match connect() {
                            Ok(c) => client.insert(c),
                            Err(e) => {
                                shared.lock().unwrap().last_error = Some(e.to_string());
                                return;
                            }
                        },
                    };
                    let ops = {
                        let mut sh = shared.lock().unwrap();
                        if sh.started >= opts.txns {
                            return;
                        }
                        sh.started += 1;
                        let ops = sh.gen.txn();
                        let line = op_line(sh.index, "invoke", pid, nanos(start), &ops, &[], list);
                        sh.history.push_str(&line);
                        sh.index += 1;
                        ops
                    };
                    let outcome = c.txn(&ops);
                    let mut sh = shared.lock().unwrap();
                    let (kind, reads) = match &outcome {
                        Outcome::Ok(r) => {
                            sh.stats.ok += 1;
                            ("ok", r.as_slice())
                        }
                        Outcome::Fail(e) => {
                            sh.stats.fail += 1;
                            sh.last_error = Some(e.clone());
                            ("fail", &[][..])
                        }
                        Outcome::Info(e) => {
                            sh.stats.info += 1;
                            sh.last_error = Some(e.clone());
                            ("info", &[][..])
                        }
                    };
                    let line = op_line(sh.index, kind, pid, nanos(start), &ops, reads, list);
                    sh.history.push_str(&line);
                    sh.index += 1;
                    drop(sh);
                    if matches!(outcome, Outcome::Info(_)) {
                        pid += opts.processes;
                        client = None;
                    }
                }
            });
        }
    });
    let sh = shared.into_inner().unwrap();
    if sh.stats.ok == 0 {
        return Err(Error::new(format!(
            "no transaction committed{}",
            sh.last_error.map(|e| format!("; last error: {e}")).unwrap_or_default()
        )));
    }
    Ok((sh.history, sh.stats))
}

fn nanos(start: Instant) -> u64 {
    start.elapsed().as_nanos() as u64
}

/// Parses a stored list (`"1,2,3"` or empty) into elements.
pub fn parse_list(s: Option<&str>) -> Vec<i64> {
    s.map(|s| s.split(',').filter(|x| !x.is_empty()).filter_map(|x| x.parse().ok()).collect()).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A client over one shared map, executing each transaction under a lock:
    /// trivially strict serializable.
    struct Locked(std::sync::Arc<Mutex<std::collections::HashMap<i64, Vec<i64>>>>);

    impl Client for Locked {
        fn txn(&mut self, ops: &[TxnOp]) -> Outcome {
            let mut m = self.0.lock().unwrap();
            Outcome::Ok(
                ops.iter()
                    .map(|op| match *op {
                        TxnOp::Append(k, v) => {
                            m.entry(k).or_default().push(v);
                            None
                        }
                        TxnOp::Write(k, v) => {
                            m.insert(k, vec![v]);
                            None
                        }
                        TxnOp::Read(k) => Some(m.get(&k).cloned().unwrap_or_default()),
                    })
                    .collect(),
            )
        }
    }

    #[test]
    fn locked_map_is_strict_serializable() {
        let store = std::sync::Arc::new(Mutex::new(Default::default()));
        let connect = move || -> Result<Box<dyn Client>, Error> { Ok(Box::new(Locked(store.clone()))) };
        let (h, stats) = run(&connect, &RunOpts { txns: 500, ..RunOpts::default() }).unwrap();
        assert_eq!(stats.ok, 500);
        let h = crate::History::from_json(&h).unwrap();
        let r = crate::check(&h, crate::Workload::ListAppend, &crate::Opts::default());
        assert_eq!(r.valid, crate::Valid::True, "{:?}", r.anomaly_types);
    }
}
