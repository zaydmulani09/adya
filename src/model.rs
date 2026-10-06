//! Consistency models, the anomalies they forbid, and how both relate.
//!
//! Two small DAGs drive everything:
//!
//! * `MODELS`: `a -> b` means every history satisfying `a` also satisfies `b`
//!   (strict serializable implies serializable, and so on).
//! * `IMPLIED`: `a -> b` means observing anomaly `a` also demonstrates `b`
//!   (a G0 cycle is also a G1c cycle, a lost update is also G2-item...).
//!
//! `PROSCRIBES` says which anomalies each model rules out directly. The
//! relations follow Adya's thesis, Bailis et al.'s HAT paper, Cerone et al.'s
//! framework and the model map at jepsen.io/consistency; names match Elle's so
//! reports from the two tools can be compared line for line.

use std::collections::{BTreeSet, VecDeque};

type Dag = &'static [(&'static str, &'static [&'static str])];

const MODELS: Dag = &[
    ("strict-serializable", &["serializable", "linearizable", "snapshot-isolation", "strong-snapshot-isolation", "strong-session-serializable", "session-serializable"]),
    ("session-serializable", &["1SR"]),
    ("serializable", &["repeatable-read", "update-serializable", "snapshot-isolation", "view-serializable"]),
    ("update-serializable", &["forward-consistent-view"]),
    ("forward-consistent-view", &["consistent-view"]),
    ("consistent-view", &["cursor-stability", "monotonic-view"]),
    ("cursor-stability", &["read-committed"]),
    ("monotonic-view", &["read-committed"]),
    ("monotonic-snapshot-read", &["read-committed"]),
    ("monotonic-atomic-view", &["read-committed"]),
    ("read-committed", &["read-uncommitted"]),
    ("repeatable-read", &["cursor-stability", "monotonic-atomic-view"]),
    ("snapshot-isolation", &["forward-consistent-view", "monotonic-atomic-view", "monotonic-snapshot-read", "parallel-snapshot-isolation", "prefix"]),
    ("parallel-snapshot-isolation", &["causal-cerone", "update-atomic"]),
    ("prefix", &["causal-cerone"]),
    ("causal-cerone", &["read-atomic"]),
    ("update-atomic", &["read-atomic"]),
    ("read-atomic", &["monotonic-atomic-view"]),
    ("strong-session-serializable", &["serializable", "strong-session-snapshot-isolation"]),
    ("strong-session-snapshot-isolation", &["snapshot-isolation", "strong-session-read-committed"]),
    ("strong-snapshot-isolation", &["strong-session-snapshot-isolation", "strong-read-committed"]),
    ("strong-session-read-committed", &["read-committed", "strong-session-read-uncommitted"]),
    ("strong-session-read-uncommitted", &["read-uncommitted"]),
    ("strong-read-committed", &["strong-session-read-committed", "strong-read-uncommitted"]),
    ("strong-read-uncommitted", &["strong-session-read-uncommitted"]),
    // Single-object models, so boundaries read naturally.
    ("linearizable", &["sequential"]),
    ("sequential", &["causal"]),
    ("causal", &["writes-follow-reads", "PRAM"]),
    ("PRAM", &["monotonic-reads", "monotonic-writes", "read-your-writes"]),
];

/// Alternative spellings accepted on input.
const ALIASES: &[(&str, &str)] = &[
    ("strong-serializable", "strict-serializable"),
    ("conflict-serializable", "serializable"),
    ("PL-SS", "strict-serializable"),
    ("PL-3", "serializable"),
    ("PL-2.99", "repeatable-read"),
    ("PL-SI", "snapshot-isolation"),
    ("PL-2", "read-committed"),
    ("PL-1", "read-uncommitted"),
];

const PROSCRIBES: Dag = &[
    ("causal-cerone", &["internal", "G1a"]),
    ("cursor-stability", &["G1", "G-cursor", "lost-update"]),
    ("monotonic-view", &["G1", "G-monotonic"]),
    ("monotonic-snapshot-read", &["G1", "G-MSR"]),
    ("consistent-view", &["G1", "G-single"]),
    ("forward-consistent-view", &["G1", "G-SIb"]),
    ("parallel-snapshot-isolation", &["internal", "G1a"]),
    ("serializable", &["G1", "G2", "internal", "predicate-read-miss", "PL-3-cycle-exists"]),
    ("read-committed", &["G1", "PL-2-cycle-exists"]),
    ("read-uncommitted", &["G0", "duplicate-elements", "cyclic-versions", "PL-1-cycle-exists"]),
    ("prefix", &["internal", "G1a"]),
    ("snapshot-isolation", &["internal", "G1", "G-SI", "G-nonadjacent", "PL-SI-cycle-exists"]),
    ("read-atomic", &["internal", "G1a", "future-read"]),
    ("repeatable-read", &["G1", "G2-item", "lost-update", "PL-2.99-cycle-exists"]),
    ("update-atomic", &["lost-update"]),
    ("strict-serializable", &["G1", "G1c-realtime", "G2-realtime", "PL-SS-cycle-exists"]),
    ("strong-session-read-uncommitted", &["G0-process", "strong-session-PL-1-cycle-exists"]),
    ("strong-session-read-committed", &["G1c-process", "strong-session-PL-2-cycle-exists"]),
    ("strong-read-uncommitted", &["G0-realtime", "strong-PL-1-cycle-exists"]),
    ("strong-read-committed", &["G1c-realtime", "strong-PL-2-cycle-exists"]),
    ("strong-session-snapshot-isolation", &["internal", "G1-process", "G-nonadjacent-process", "strong-session-snapshot-isolation-cycle-exists"]),
    ("strong-snapshot-isolation", &["internal", "G1-realtime", "G-nonadjacent-realtime", "strong-snapshot-isolation-cycle-exists"]),
    ("strong-session-serializable", &["G1-process", "G2-process", "strong-session-serializable-cycle-exists"]),
    ("update-serializable", &["G1", "G-update"]),
];

const IMPLIED: Dag = &[
    ("G0", &["G1c", "G0-process"]),
    ("G0-process", &["G1c-process", "G0-realtime"]),
    ("G0-realtime", &["G1c-realtime"]),
    ("G1a", &["G1"]),
    ("G1b", &["G1"]),
    ("G1c", &["G1", "G1c-process"]),
    ("G1c-process", &["G1c-realtime", "G1-process"]),
    ("G1c-realtime", &["G1-realtime"]),
    ("G1", &["G1-process"]),
    ("G1-process", &["G1-realtime"]),
    ("G-single-item", &["G-single", "G-single-item-process", "G-nonadjacent-item"]),
    ("G-single-item-process", &["G-single-process", "G-single-item-realtime", "G-nonadjacent-item-process"]),
    ("G-single-item-realtime", &["G-single-realtime", "G-nonadjacent-item-realtime"]),
    ("G-single", &["G-nonadjacent", "GSIb", "G-single-process"]),
    ("G-single-process", &["G-nonadjacent-process", "G-single-realtime"]),
    ("G-single-realtime", &["G-nonadjacent-realtime"]),
    ("G-nonadjacent-item", &["G-nonadjacent", "G2-item", "G-nonadjacent-item-process"]),
    ("G-nonadjacent-item-process", &["G-nonadjacent-process", "G2-item-process", "G-nonadjacent-item-realtime"]),
    ("G-nonadjacent-item-realtime", &["G2-item-realtime", "G-nonadjacent-realtime"]),
    ("G-nonadjacent", &["G2", "G-nonadjacent-process"]),
    ("G-nonadjacent-process", &["G2-process", "G-nonadjacent-realtime"]),
    ("G-nonadjacent-realtime", &["G2-realtime"]),
    ("G2", &["G2-process"]),
    ("G2-item", &["G2", "G2-item-process"]),
    ("G2-item-process", &["G2-process", "G2-item-realtime"]),
    ("G2-item-realtime", &["G2-realtime"]),
    ("G2-process", &["G2-realtime"]),
    ("GSIa", &["GSI"]),
    ("GSIb", &["GSI"]),
    ("incompatible-order", &["G1a"]),
    ("future-read", &["G1c"]),
    ("dirty-update", &["G1a"]),
    ("lost-update", &["write-skew", "G2-item"]),
    ("write-skew", &["G2"]),
];

fn out(dag: Dag, v: &str) -> Vec<&'static str> {
    dag.iter().filter(|(a, _)| *a == v).flat_map(|(_, bs)| bs.iter().copied()).collect()
}

fn inbound(dag: Dag, v: &str) -> Vec<&'static str> {
    dag.iter().filter(|(_, bs)| bs.contains(&v)).map(|(a, _)| *a).collect()
}

/// Every vertex reachable from `start` (inclusive), following `step`.
fn closure<'a>(start: impl IntoIterator<Item = &'a str>, step: impl Fn(&str) -> Vec<&'static str>) -> BTreeSet<String> {
    let mut seen = BTreeSet::new();
    let mut queue: VecDeque<String> = start.into_iter().map(String::from).collect();
    while let Some(v) = queue.pop_front() {
        if seen.insert(v.clone()) {
            queue.extend(step(&v).into_iter().map(String::from));
        }
    }
    seen
}

/// Resolves aliases and checks the model is known.
pub fn canonical(model: &str) -> Option<&'static str> {
    let m = ALIASES.iter().find(|(a, _)| *a == model).map_or(model, |(_, b)| b);
    MODELS
        .iter()
        .flat_map(|(a, bs)| std::iter::once(*a).chain(bs.iter().copied()))
        .find(|x| *x == m)
}

/// All model names, for help text.
pub fn all_models() -> BTreeSet<&'static str> {
    MODELS.iter().flat_map(|(a, bs)| std::iter::once(*a).chain(bs.iter().copied())).collect()
}

/// Anomalies which, if present, mean the given models do not all hold.
pub fn prohibited_by(models: &[&str]) -> BTreeSet<String> {
    let implied_models = closure(models.iter().copied(), |m| out(MODELS, m));
    let direct: Vec<&'static str> = implied_models.iter().flat_map(|m| out(PROSCRIBES, m)).collect();
    // Anything that implies a forbidden anomaly is forbidden too.
    closure(direct, |a| inbound(IMPLIED, a))
}

/// Anomalies implied by the given ones (inclusive).
pub fn implied(anomalies: &[&str]) -> BTreeSet<String> {
    closure(anomalies.iter().copied(), |a| out(IMPLIED, a))
}

/// Models ruled out by the presence of these anomalies.
pub fn impossible_models(anomalies: &[&str]) -> BTreeSet<String> {
    let all = implied(anomalies);
    let direct: Vec<&'static str> = all.iter().flat_map(|a| inbound(PROSCRIBES, a)).collect();
    closure(direct, |m| inbound(MODELS, m))
}

/// Splits impossible models into the weakest ones (`not`) and everything
/// stronger (`also_not`), like Elle's `:not` / `:also-not`.
pub fn boundary(anomalies: &[&str]) -> (BTreeSet<String>, BTreeSet<String>) {
    let impossible = impossible_models(anomalies);
    let weakest: BTreeSet<String> = impossible
        .iter()
        .filter(|m| {
            // Weakest: implies no other impossible model.
            let below = closure([m.as_str()], |x| out(MODELS, x));
            !below.iter().any(|b| b != *m && impossible.contains(b))
        })
        .cloned()
        .collect();
    let rest = impossible.difference(&weakest).cloned().collect();
    (weakest, rest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializable_forbids_write_skew_but_si_does_not() {
        let ser = prohibited_by(&["serializable"]);
        let si = prohibited_by(&["snapshot-isolation"]);
        assert!(ser.contains("G2-item") && ser.contains("G-single-item"));
        assert!(si.contains("G-single-item") && si.contains("G0"));
        assert!(!si.contains("G2-item"));
        assert!(!ser.contains("G1c-realtime"));
        assert!(prohibited_by(&["strict-serializable"]).contains("G1c-realtime"));
    }

    #[test]
    fn boundary_of_write_skew() {
        let (not, also) = boundary(&["G2-item"]);
        assert_eq!(not.into_iter().collect::<Vec<_>>(), ["repeatable-read"]);
        assert!(also.contains("serializable") && also.contains("strict-serializable"));
        assert!(!also.contains("snapshot-isolation"));
    }

    #[test]
    fn boundary_of_g_single() {
        let (not, _) = boundary(&["G-single-item"]);
        assert!(not.contains("consistent-view"), "{not:?}");
    }

    #[test]
    fn aliases() {
        assert_eq!(canonical("strong-serializable"), Some("strict-serializable"));
        assert_eq!(canonical("serializable"), Some("serializable"));
        assert_eq!(canonical("nope"), None);
    }
}
