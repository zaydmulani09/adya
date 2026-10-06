use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand, ValueEnum};

use adya::{check, History, Opts, Report, Valid, Workload};

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

#[derive(Subcommand)]
enum Cmd {
    /// Check histories. Exit status: 0 valid, 1 anomalies found, 2 unknown,
    /// 3 error.
    Check {
        /// History files (JSON lines or a JSON array, as elle-cli reads).
        #[arg(required = true)]
        files: Vec<PathBuf>,
        /// Workload the history came from.
        #[arg(short, long, value_enum, default_value = "list-append")]
        model: Model,
        /// Consistency models the history must satisfy (repeatable).
        #[arg(short = 'c', long = "consistency-models", default_value = "strict-serializable", value_delimiter = ',')]
        consistency_models: Vec<String>,
        /// Extra anomalies to treat as failures (repeatable), e.g. G1a.
        #[arg(short, long, value_delimiter = ',')]
        anomalies: Vec<String>,
        /// Cycle search budget per strongly connected component, in ms.
        #[arg(long = "cycle-search-timeout", default_value_t = 1000)]
        timeout_ms: u64,
        /// rw-register: assume writes follow reads within a transaction.
        #[arg(long)]
        wfr_keys: bool,
        /// Print the full report as JSON.
        #[arg(long)]
        json: bool,
    },
}

fn main() -> ExitCode {
    match Cli::parse().cmd {
        Cmd::Check { files, model, consistency_models, anomalies, timeout_ms, wfr_keys, json } => {
            for m in &consistency_models {
                if adya::model::canonical(m).is_none() {
                    eprintln!("unknown consistency model {m:?}; known models:");
                    eprintln!("  {}", adya::model::all_models().into_iter().collect::<Vec<_>>().join(", "));
                    return ExitCode::from(3);
                }
            }
            let models = consistency_models.iter().map(|m| adya::model::canonical(m).unwrap().to_string()).collect();
            let opts = Opts { models, anomalies, timeout: Duration::from_millis(timeout_ms), wfr_keys };
            let workload = match model {
                Model::ListAppend => Workload::ListAppend,
                Model::RwRegister => Workload::RwRegister,
            };
            let mut worst = 0u8;
            for f in &files {
                let code = match std::fs::read_to_string(f).map_err(|e| e.to_string()).and_then(|t| History::from_json(&t).map_err(|e| e.to_string())) {
                    Err(e) => {
                        eprintln!("{}: {e}", f.display());
                        3
                    }
                    Ok(h) => {
                        let r = check(&h, workload, &opts);
                        print(f, &r, json);
                        match r.valid {
                            Valid::True => 0,
                            Valid::False => 1,
                            Valid::Unknown => 2,
                        }
                    }
                };
                worst = worst.max(code);
            }
            ExitCode::from(worst)
        }
    }
}

fn print(f: &std::path::Path, r: &Report, json: bool) {
    if json {
        println!("{}", serde_json::to_string_pretty(r).unwrap());
        return;
    }
    let v = match r.valid {
        Valid::True => "true",
        Valid::False => "false",
        Valid::Unknown => "unknown",
    };
    println!("{}\t{v}", f.display());
    for (kind, list) in &r.anomalies {
        for (i, a) in list.iter().enumerate() {
            println!("\n{kind} #{i}");
            for line in a.explanation.lines() {
                println!("  {line}");
            }
        }
    }
    if !r.not.is_empty() {
        println!("\nNot {}.", r.not.join(", "));
        if !r.also_not.is_empty() {
            println!("Also not {}.", r.also_not.join(", "));
        }
    }
}
