# adya

**A black-box checker for transactional isolation.** Point it at a history of
transactions your database executed and it tells you which isolation
guarantees the database actually kept, with a step-by-step proof for every
violation it finds: G0, G1a/b/c, G-single, G-nonadjacent, G2, lost update, and
their process and real-time variants.

adya is an independent Rust implementation of the approach behind Jepsen's
[Elle](https://github.com/jepsen-io/elle) (Kingsbury & Alvaro, VLDB 2020). It
ships as one native binary with no JVM, reads the same history formats, and
includes the half Elle leaves to you: a workload runner for SQLite, Postgres,
MySQL, or any database you can drive from a subprocess.

```console
$ adya run sim -i snapshot-isolation -c serializable -n 300 -p 4 --seed 1
history.jsonl	false

G2-item #0
  Let:
    T234 = {"index":234,"process":1,"type":"ok","value":[["r",10,[4]],["append",3,6],["r",3,[4,5,6]]]}
    T237 = {"index":237,"process":3,"type":"ok","value":[["append",9,13],["append",10,5],["r",3,[4,5]],["r",10,[4,5]]]}
  Then:
    - T234 < T237, because T234 did not observe T237's append of 5 to 10.
    - However, T237 < T234, because T237 did not observe T234's append of 6 to 3: a contradiction!
...
Not repeatable-read.
```

That is write skew: each transaction read a key the other was about to
change, which snapshot isolation permits and serializability forbids. adya
found it from the outside, from nothing but what clients wrote and read; the
same check runs against Postgres, MySQL, SQLite or anything else.

## Why

Elle is the state of the art for finding isolation bugs; it has found
violations in dozens of databases. But using it outside a Jepsen test means a
JVM in your CI, a Clojure toolchain or `elle-cli`, and writing your own
workload driver. Projects that want it end up shelling out to a JVM from a Go
or Python harness ([barn](https://github.com/MongooseMoo/barn/issues/412),
[bytecaskdb](https://github.com/gustavoamigo/bytecaskdb/pull/180)), and run
into blocked Clojars mirrors, graphviz crashes, and logs interleaved into the
JSON output. On the Windows machine where adya was written, elle-cli 0.1.11
hangs on every history that contains an anomaly.

adya is the same idea in a form you can drop into any pipeline:

* **one static binary** (`cargo install adya`), or a Rust library;
* **reads Elle's formats**: elle-cli JSON, JSON lines, and Jepsen's
  `history.edn`, with elle-cli compatible flags (`-m`, `-c`, `-a`,
  `--cycle-search-timeout`);
* **runs the workload too**: `adya run` drives SQLite, Postgres or MySQL with
  concurrent clients, records an Elle-format history, and checks it;
* **speaks to anything** through `adya run exec`, a one-line-per-transaction
  JSON protocol your database client can implement in any language;
* **fast**: a 100,000-transaction history (200k operations, 30 MB) checks in
  about 1.6 seconds on a laptop.

## Install

```bash
cargo install adya
```

Or build from source with `cargo build --release`. The database drivers are
default features (`sqlite`, `postgres`, `mysql`); `--no-default-features`
builds just the checker.

## Try it in ten seconds

The built-in simulated database implements isolation levels the textbook way,
so you can watch adya separate them without installing anything:

```bash
# Snapshot isolation permits write skew; serializability does not.
adya run sim -i snapshot-isolation -c snapshot-isolation   # valid
adya run sim -i snapshot-isolation -c serializable         # G2-item

# Read committed lets transactions see half of another's effects.
adya run sim -i read-committed -c snapshot-isolation       # G-single-item

# A buggy "snapshot" that writes back stale state.
adya run sim -i lost-update -c read-committed              # G1a, incompatible-order, ...
```

## Checking an existing history

```bash
adya check -m list-append -c serializable history.jsonl
adya check -m rw-register -c snapshot-isolation history.edn
adya check --json history.jsonl     # machine-readable report
```

Exit status is `0` if the history satisfies every requested model, `1` if
anomalies were found, `2` if the result is unknown (for example, no
dependencies could be inferred, or cycle search timed out), and `3` on error.

A history is a sequence of operations, each a transaction invocation or
completion on a process:

```json
{"index":0,"type":"invoke","process":0,"value":[["append",1,1],["r",2,null]]}
{"index":1,"type":"ok","process":0,"value":[["append",1,1],["r",2,[3,7]]]}
```

* `type` is `invoke`, then `ok` (committed), `fail` (definitely aborted) or
  `info` (unknown).
* **list-append** micro-ops are `["append", key, value]` and
  `["r", key, [elements...]]`. Appended values must be unique per key. This is
  the workload to use if you can: every read reveals the order of all the
  appends before it, so adya can recover the full version order.
* **rw-register** micro-ops are `["w", key, value]` and `["r", key, value]`.
  Registers hide their history, so far less can be inferred. adya knows the
  initial `nil` precedes everything, and with `--wfr-keys` assumes a
  transaction's writes follow its reads.

Consistency models include `strict-serializable` (default), `serializable`,
`snapshot-isolation`, `repeatable-read`, `read-committed`,
`read-uncommitted`, their `strong-session-*` and `strong-*` variants,
`cursor-stability`, `consistent-view`, `update-serializable`, and others; see
`adya check --help`.

## Testing a database

```bash
adya run sqlite   --url test.sqlite -i immediate
adya run postgres --url postgres://user:pass@localhost/db -i serializable
adya run mysql    --url mysql://user:pass@localhost/db    -i repeatable-read -c snapshot-isolation
```

Each run creates two tables, `adya_lists (k, v)` and `adya_regs (k, v)`, and
runs `--txns` random transactions (default 2000) from `--processes`
concurrent clients (default 8) over a small, rotating pool of `--keys` hot
keys. Appends are upserts that concatenate onto a text column. Errors before
`COMMIT` are recorded as failures; losing the connection during `COMMIT` is
recorded as indeterminate, and that client reconnects under a new process id,
as in Jepsen. The history is written to `history.jsonl` (`-o` to change it)
and checked against `-c`.

### Any other database: `adya run exec`

```bash
adya run exec --url "python my_client.py" -c serializable
```

adya starts the command once per client and writes one line per
transaction to its stdin:

```json
{"value":[["append",3,7],["r",4,null]]}
```

The client runs it as one transaction and answers with one line:

```json
{"type":"ok","value":[["append",3,7],["r",4,[1,7]]]}
{"type":"fail","error":"serialization failure"}
{"type":"info","error":"connection reset during commit"}
```

Use `fail` only when the transaction certainly did not commit. For
rw-register workloads (`-m rw-register`) reads return a number or `null`.

## Using the library

```rust
use adya::{check, History, Opts, Valid, Workload};

let history = History::parse(&std::fs::read_to_string("history.edn")?)?;
let report = check(&history, Workload::ListAppend, &Opts {
    models: vec!["serializable".into()],
    ..Opts::default()
});
if report.valid == Valid::False {
    for (kind, anomalies) in &report.anomalies {
        for a in anomalies {
            eprintln!("{kind}:\n{}", a.explanation);
        }
    }
}
```

`adya::run` and `adya::db` expose the workload runner and drivers; implement
`adya::run::Client` (one method: run a transaction, report how it ended) to
test a database from Rust.

## How it works

1. **Parse** the history and pair each invocation with its completion.
2. **Infer version orders.** For list-append, each key's longest read is its
   version order; reads that aren't prefixes of each other are themselves an
   anomaly (`incompatible-order`). For rw-register, the order comes from the
   initial state and, optionally, writes-follow-reads.
3. **Find direct anomalies** that need no graph: aborted reads (G1a),
   intermediate reads (G1b), reads inconsistent with the transaction's own
   writes (`internal`), duplicated elements, dirty updates, lost updates.
4. **Build Adya's dependency graph** over committed (and possibly committed)
   transactions: `ww` (installed the next version), `wr` (read what the other
   wrote), `rw` (read a version the other overwrote). When the requested
   models need it, add process order or real-time order. Real-time order uses
   a transitive reduction (each transaction links only to the frontier of
   recent completions), so it stays linear in the history.
5. **Bound what is possible.** For each model, check whether the subgraph it
   forbids cycles in has a nontrivial strongly connected component at all.
   This is cheap, and it rules out whole families of anomalies before any
   search.
6. **Search for the worst anomaly first.** Inside each SCC, run a
   breadth-first search over (transaction, path-state) pairs, where the path
   state tracks how many `rw` edges the path has used, whether two were
   adjacent, and whether it has used a `wr` or a real-time edge. One search
   then finds, say, a shortest cycle with *exactly one* anti-dependency
   (G-single), with *only nonadjacent* ones (G-nonadjacent), or with an
   adjacent pair (G2-item). Anything implied by what was found is skipped.
7. **Report** each cycle with a proof that names the key and value behind
   every edge, plus the weakest models the anomalies rule out (`not`) and
   everything stronger (`also not`).

## Is it right?

The checker is held to three independent standards in CI:

* **Elle's expected results.** elle-cli ships histories with the verdicts Elle
  produced for them. adya matches all 50 that use features it supports, on
  verdict, anomaly types *and* the weakest models ruled out
  (`scripts/compare_elle_results.py`). Six cases use version-order inference
  adya doesn't implement yet (see Limitations).
* **Elle itself, live.** On every push, CI generates random histories across
  isolation levels and workloads, and checks each one with both adya and the
  JVM Elle (`scripts/differential.py`).
* **Databases with known answers.** The simulated database implements
  serializable, snapshot isolation, read committed (with write locks), read
  uncommitted, and a broken snapshot mode. Tests assert, over thousands of
  transactions per seed, that correct runs produce *no* anomalies at their
  level and that the expected anomalies do appear above it
  (`tests/sim.rs`).

## Limitations

* rw-register version inference covers the initial state and
  writes-follow-reads. Elle's `sequential-keys`, `linearizable-keys` and
  `transaction-order` sources are not implemented yet.
* Predicate workloads (and so the predicate variants G2/G-single proper, as
  opposed to the item variants) and Elle's other checkers (bank, set, long
  fork, counters) are out of scope for now.
* No graph rendering. Each anomaly comes with a textual proof and JSON detail
  instead of Graphviz plots.
* The SQL drivers connect without TLS.
* A clean result means no anomaly was observed in this history. It is not a
  proof that the database is correct. Run longer, with more contention
  (fewer `--keys`), and with faults.

## Roadmap

* Fault injection during `adya run`: kill and pause clients and servers,
  partition through a proxy.
* The remaining rw-register version sources, then predicate reads.
* A linearizability checker for single-object histories.
* `--directory` output with per-anomaly files and DOT graphs, as in Elle.

## Credits

The analysis follows Atul Adya's *Weak Consistency: A Generalized Theory and
Optimistic Implementations for Distributed Transactions* (MIT, 1999) and
Kingsbury & Alvaro's *Elle: Inferring Isolation Anomalies from Experimental
Observations* (VLDB 2020). Anomaly and model names follow Elle's so reports
from the two tools can be compared directly. adya is an independent
implementation and is not affiliated with Jepsen.

## License

MIT or Apache-2.0, at your option.
