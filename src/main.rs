use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use clap::{Args, Parser, Subcommand, ValueEnum};

use adya::gen::{GenOpts, Kind};
use adya::run::{Client, RunOpts};
use adya::{check, Error, History, Opts, Report, Valid, Workload};

#[derive(Parser)]
#[command(name = "adya", version, about = "Find isolation anomalies in database transaction histories")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Clone, Copy, ValueEnum)]
enum Model {
    ListAppend,
    RwRegister,
}

impl Model {
    fn workload(self) -> Workload {
        match self {
            Model::ListAppend => Workload::ListAppend,
            Model::RwRegister => Workload::RwRegister,
        }
    }
}

#[derive(Args)]
struct CheckArgs {
    /// Workload the history came from.
    #[arg(short, long, value_enum, default_value = "list-append")]
    model: Model,
    /// Consistency models the history must satisfy (comma-separated or repeated).
    #[arg(short = 'c', long = "consistency-models", default_value = "strict-serializable", value_delimiter = ',')]
    consistency_models: Vec<String>,
    /// Extra anomalies to treat as failures, e.g. G1a.
    #[arg(short, long, value_delimiter = ',')]
    anomalies: Vec<String>,
    /// Cycle search budget per strongly connected component, in ms.
    #[arg(long = "cycle-search-timeout", default_value_t = 1000)]
    timeout_ms: u64,
    /// rw-register: assume writes follow reads within a transaction.
    #[arg(long)]
    wfr_keys: bool,
    /// rw-register: assume each key is sequentially consistent (process
    /// order implies version order).
    #[arg(long)]
    sequential_keys: bool,
    /// rw-register: assume each key is linearizable (real-time order implies
    /// version order).
    #[arg(long)]
    linearizable_keys: bool,
    /// rw-register: a JSON file mapping completion op indices to the
    /// database's own commit order, e.g. {"1": 0, "3": 1}.
    #[arg(long, value_name = "FILE")]
    transaction_order: Option<PathBuf>,
    /// Print the full report as JSON.
    #[arg(long)]
    json: bool,
}

#[derive(Clone, Copy, ValueEnum, PartialEq, Eq)]
enum Target {
    /// The built-in simulated database (no setup needed).
    Sim,
    Sqlite,
    Postgres,
    Mysql,
    /// Any database, through a child process speaking JSON lines (see docs).
    Exec,
}

#[derive(Subcommand)]
enum Cmd {
    /// Check histories. Exit status: 0 valid, 1 anomalies found, 2 unknown,
    /// 3 error.
    Check {
        /// History files: JSON lines, a JSON array, or Jepsen EDN.
        #[arg(required = true)]
        files: Vec<PathBuf>,
        #[command(flatten)]
        check: CheckArgs,
    },
    /// Run a workload against a database, record the history, and check it.
    Run {
        #[arg(value_enum)]
        target: Target,
        /// Connection URL (postgres://, mysql://), SQLite file path, or for
        /// `exec` the command to run.
        #[arg(long)]
        url: Option<String>,
        /// Isolation level. SQL databases: read-uncommitted, read-committed,
        /// repeatable-read, serializable. SQLite: deferred, immediate,
        /// exclusive. sim: serializable, snapshot-isolation, read-committed,
        /// read-uncommitted, lost-update.
        #[arg(short, long, default_value = "serializable")]
        isolation: String,
        /// Concurrent clients.
        #[arg(short, long, default_value_t = 8)]
        processes: usize,
        /// Transactions to run.
        #[arg(short = 'n', long, default_value_t = 2000)]
        txns: usize,
        /// Stop after this many seconds even if transactions remain.
        #[arg(long)]
        time_limit: Option<u64>,
        /// Keys in play at once (fewer keys, more contention).
        #[arg(long, default_value_t = 8)]
        keys: usize,
        /// Writes per key before it is retired for a fresh one.
        #[arg(long, default_value_t = 32)]
        max_writes_per_key: u64,
        #[arg(long, default_value_t = 0)]
        seed: u64,
        /// Shell command that injects a fault, e.g. "docker pause pg".
        #[arg(long, requires = "heal")]
        fault: Option<String>,
        /// Shell command that heals it, e.g. "docker unpause pg".
        #[arg(long, requires = "fault")]
        heal: Option<String>,
        /// Seconds between faults.
        #[arg(long, default_value_t = 10)]
        fault_every: u64,
        /// Seconds each fault lasts before healing.
        #[arg(long, default_value_t = 5)]
        fault_for: u64,
        /// Where to write the history.
        #[arg(short, long, default_value = "history.jsonl")]
        out: PathBuf,
        #[command(flatten)]
        check: CheckArgs,
    },
}

fn main() -> ExitCode {
    let code = match Cli::parse().cmd {
        Cmd::Check { files, check } => check_files(&files, &check),
        Cmd::Run {
            target,
            url,
            isolation,
            processes,
            txns,
            time_limit,
            keys,
            max_writes_per_key,
            seed,
            fault,
            heal,
            fault_every,
            fault_for,
            out,
            check,
        } => {
            let kind = match check.model {
                Model::ListAppend => Kind::ListAppend,
                Model::RwRegister => Kind::RwRegister,
            };
            let gen = GenOpts { kind, key_count: keys.max(1), max_writes_per_key, ..GenOpts::default() };
            let opts = RunOpts {
                processes: processes.max(1),
                txns,
                time_limit: time_limit.map(Duration::from_secs),
                gen,
                seed,
                nemesis: fault.zip(heal).map(|(fault, heal)| adya::run::Nemesis {
                    fault,
                    heal,
                    every: Duration::from_secs(fault_every),
                    duration: Duration::from_secs(fault_for),
                }),
            };
            match record(target, url, &isolation, &opts) {
                Err(e) => {
                    eprintln!("error: {e}");
                    3
                }
                Ok(history) => match std::fs::write(&out, history) {
                    Err(e) => {
                        eprintln!("error: writing {}: {e}", out.display());
                        3
                    }
                    Ok(()) => {
                        eprintln!("wrote {}", out.display());
                        check_files(&[out], &check)
                    }
                },
            }
        }
    };
    ExitCode::from(code)
}

fn record(target: Target, url: Option<String>, isolation: &str, opts: &RunOpts) -> Result<String, Error> {
    #[allow(unused_variables)] // only the SQL drivers need it
    let list = opts.gen.kind == Kind::ListAppend;
    if target == Target::Sim {
        let iso = adya::sim::Isolation::parse(isolation)
            .ok_or_else(|| Error::new(format!("unknown sim isolation {isolation:?}")))?;
        let sim = adya::sim::SimOpts {
            isolation: iso,
            processes: opts.processes,
            txns: opts.txns,
            gen: opts.gen.clone(),
            seed: opts.seed,
            ..Default::default()
        };
        return Ok(adya::sim::run(&sim));
    }
    let url = url.ok_or_else(|| Error::new("--url is required for this target"))?;
    let connect: Box<dyn Fn() -> Result<Box<dyn Client>, Error> + Sync> = match target {
        Target::Sim => unreachable!(),
        #[cfg(feature = "sqlite")]
        Target::Sqlite => {
            adya::db::sqlite::prepare(&url)?;
            let mode = if isolation == "serializable" { "deferred".to_string() } else { isolation.to_string() };
            Box::new(move || adya::db::sqlite::connect(&url, &mode, list))
        }
        #[cfg(feature = "postgres")]
        Target::Postgres => {
            adya::db::postgres::prepare(&url)?;
            let level = isolation.to_string();
            Box::new(move || adya::db::postgres::connect(&url, &level, list))
        }
        #[cfg(feature = "mysql")]
        Target::Mysql => {
            adya::db::mysql::prepare(&url)?;
            let level = isolation.to_string();
            Box::new(move || adya::db::mysql::connect(&url, &level, list))
        }
        Target::Exec => Box::new(move || adya::db::exec::connect(&url)),
        #[allow(unreachable_patterns)]
        _ => return Err(Error::new("this build of adya was compiled without that driver")),
    };
    let (history, stats) = adya::run::run(&*connect, opts)?;
    eprintln!("{} ok, {} failed, {} indeterminate", stats.ok, stats.fail, stats.info);
    Ok(history)
}

fn check_files(files: &[PathBuf], args: &CheckArgs) -> u8 {
    let mut models = Vec::new();
    for m in &args.consistency_models {
        match adya::model::canonical(m) {
            Some(c) => models.push(c.to_string()),
            None => {
                eprintln!("unknown consistency model {m:?}; known models:");
                eprintln!("  {}", adya::model::all_models().into_iter().collect::<Vec<_>>().join(", "));
                return 3;
            }
        }
    }
    let transaction_order = match &args.transaction_order {
        None => None,
        Some(p) => {
            let parsed = std::fs::read_to_string(p)
                .map_err(|e| e.to_string())
                .and_then(|t| {
                    serde_json::from_str::<std::collections::HashMap<String, i64>>(&t).map_err(|e| e.to_string())
                })
                .and_then(|m| {
                    m.into_iter().map(|(k, v)| k.parse::<u64>().map(|k| (k, v)).map_err(|e| e.to_string())).collect()
                });
            match parsed {
                Ok(m) => Some(m),
                Err(e) => {
                    eprintln!("{}: {e}", p.display());
                    return 3;
                }
            }
        }
    };
    let opts = Opts {
        transaction_order,
        models,
        anomalies: args.anomalies.clone(),
        timeout: Duration::from_millis(args.timeout_ms),
        wfr_keys: args.wfr_keys,
        sequential_keys: args.sequential_keys,
        linearizable_keys: args.linearizable_keys,
    };
    let mut worst = 0u8;
    for f in files {
        let parsed = std::fs::read_to_string(f)
            .map_err(|e| e.to_string())
            .and_then(|t| History::parse(&t).map_err(|e| e.to_string()));
        let code = match parsed {
            Err(e) => {
                eprintln!("{}: {e}", f.display());
                3
            }
            Ok(h) => {
                let r = check(&h, args.model.workload(), &opts);
                print(f, &r, args.json);
                match r.valid {
                    Valid::True => 0,
                    Valid::False => 1,
                    Valid::Unknown => 2,
                }
            }
        };
        worst = worst.max(code);
    }
    worst
}

fn print(f: &Path, r: &Report, json: bool) {
    // Ignore write errors: a closed pipe (`| head`) is not a failure.
    use std::io::Write;
    let mut o = std::io::stdout().lock();
    if json {
        let _ = writeln!(o, "{}", serde_json::to_string_pretty(r).unwrap());
        return;
    }
    let v = match r.valid {
        Valid::True => "true",
        Valid::False => "false",
        Valid::Unknown => "unknown",
    };
    let _ = writeln!(o, "{}\t{v}", f.display());
    // A few examples of each kind are enough to act on; --json has them all.
    const SHOWN: usize = 3;
    for (kind, list) in &r.anomalies {
        for (i, a) in list.iter().take(SHOWN).enumerate() {
            let _ = writeln!(o, "\n{kind} #{i}");
            for line in a.explanation.lines() {
                let _ = writeln!(o, "  {line}");
            }
        }
        if list.len() > SHOWN {
            let _ = writeln!(o, "\n... and {} more {kind} (see --json)", list.len() - SHOWN);
        }
    }
    if !r.anomalies.is_empty() {
        let counts: Vec<String> = r.anomalies.iter().map(|(k, v)| format!("{} {k}", v.len())).collect();
        let _ = writeln!(o, "\nFound {}.", counts.join(", "));
    }
    if !r.not.is_empty() {
        let _ = writeln!(o, "\nNot {}.", r.not.join(", "));
        if !r.also_not.is_empty() {
            let _ = writeln!(o, "Also not {}.", r.also_not.join(", "));
        }
    }
}
