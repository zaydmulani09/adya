//! The simulated database implements each isolation level the textbook way,
//! so its histories have known answers. These tests look for false positives
//! (anomalies reported on a correct run) and missed anomalies.

use adya::gen::{GenOpts, Kind};
use adya::sim::{run, Isolation, SimOpts};
use adya::{check, History, Opts, Report, Valid, Workload};

fn sim(isolation: Isolation, kind: Kind, seed: u64) -> History {
    let opts =
        SimOpts { isolation, txns: 2000, seed, gen: GenOpts { kind, ..GenOpts::default() }, ..SimOpts::default() };
    History::from_json(&run(&opts)).unwrap()
}

fn verdict(h: &History, kind: Kind, model: &str) -> Report {
    let w = if kind == Kind::ListAppend { Workload::ListAppend } else { Workload::RwRegister };
    check(h, w, &Opts { models: vec![model.into()], ..Opts::default() })
}

#[test]
fn serializable_runs_are_strict_serializable() {
    for kind in [Kind::ListAppend, Kind::RwRegister] {
        for seed in 0..5 {
            let r = verdict(&sim(Isolation::Serializable, kind, seed), kind, "strict-serializable");
            assert_eq!(r.valid, Valid::True, "{kind:?} seed {seed}: {:?}", r.anomaly_types);
        }
    }
}

#[test]
fn snapshot_isolation_allows_write_skew_only() {
    let mut skew = 0;
    for seed in 0..5 {
        let h = sim(Isolation::SnapshotIsolation, Kind::ListAppend, seed);
        let si = verdict(&h, Kind::ListAppend, "snapshot-isolation");
        assert_eq!(si.valid, Valid::True, "seed {seed}: {:?}", si.anomaly_types);
        let ser = verdict(&h, Kind::ListAppend, "serializable");
        if ser.valid == Valid::False {
            assert!(ser.anomaly_types.iter().any(|t| t == "G2-item"), "{:?}", ser.anomaly_types);
            assert_eq!(ser.not, ["repeatable-read"]);
            skew += 1;
        }
    }
    assert!(skew >= 3, "write skew should show up in most runs, saw {skew}/5");
}

#[test]
fn read_committed_is_caught_by_snapshot_isolation() {
    for seed in 0..5 {
        let h = sim(Isolation::ReadCommitted, Kind::ListAppend, seed);
        assert_eq!(verdict(&h, Kind::ListAppend, "read-committed").valid, Valid::True, "seed {seed}");
        let si = verdict(&h, Kind::ListAppend, "snapshot-isolation");
        assert_eq!(si.valid, Valid::False, "seed {seed}");
        assert!(
            si.anomaly_types.iter().any(|t| t == "G-single-item" || t == "G-nonadjacent-item"),
            "{:?}",
            si.anomaly_types
        );
    }
}

#[test]
fn read_uncommitted_shows_aborted_reads() {
    let mut g1a = 0;
    for seed in 0..5 {
        let h = sim(Isolation::ReadUncommitted, Kind::ListAppend, seed);
        let r = verdict(&h, Kind::ListAppend, "read-committed");
        assert_eq!(r.valid, Valid::False);
        if r.anomaly_types.iter().any(|t| t == "G1a") {
            g1a += 1;
        }
    }
    assert!(g1a >= 3, "saw G1a in {g1a}/5 runs");
}

#[test]
fn broken_snapshot_loses_updates() {
    for seed in 0..5 {
        let h = sim(Isolation::LostUpdate, Kind::ListAppend, seed);
        let r = verdict(&h, Kind::ListAppend, "read-committed");
        assert_eq!(r.valid, Valid::False, "seed {seed}");
        let t = &r.anomaly_types;
        assert!(t.iter().any(|t| t == "incompatible-order" || t == "G1a" || t.starts_with("G0")), "{t:?}");
    }
}
