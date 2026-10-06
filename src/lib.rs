//! adya: a black-box checker for transactional isolation.
//!
//! Give it a history of transactions a database executed, and it infers the
//! dependency graph between them (Adya's write-write, write-read and
//! read-write edges, plus process and real-time order), then hunts for the
//! cycles that each isolation level forbids.
//!
//! ```
//! use adya::{check, History, Opts, Valid, Workload};
//!
//! // Two transactions each read both keys as empty, then append to a
//! // different one: write skew, legal under snapshot isolation only.
//! let h = History::from_json(r#"
//! {"type":"invoke","process":0,"value":[["r","x",null],["r","y",null],["append","x",1]]}
//! {"type":"invoke","process":1,"value":[["r","x",null],["r","y",null],["append","y",1]]}
//! {"type":"ok","process":0,"value":[["r","x",[]],["r","y",[]],["append","x",1]]}
//! {"type":"ok","process":1,"value":[["r","x",[]],["r","y",[]],["append","y",1]]}
//! "#).unwrap();
//!
//! let serializable = Opts { models: vec!["serializable".into()], ..Opts::default() };
//! let r = check(&h, Workload::ListAppend, &serializable);
//! assert_eq!(r.valid, Valid::False);
//! assert_eq!(r.anomaly_types, ["G2-item"]);
//!
//! let si = Opts { models: vec!["snapshot-isolation".into()], ..Opts::default() };
//! assert_eq!(check(&h, Workload::ListAppend, &si).valid, Valid::True);
//! ```

pub mod check;
pub mod gen;
pub mod graph;
pub mod history;
pub mod list_append;
pub mod model;
pub mod run;
pub mod rw_register;
pub mod sim;

pub use check::{check, Anomaly, Opts, Report, Valid, Workload};
pub use history::History;

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    pub msg: String,
}

impl Error {
    pub fn new(msg: impl Into<String>) -> Error {
        Error { msg: msg.into() }
    }

    pub(crate) fn at(line: usize, msg: impl fmt::Display) -> Error {
        Error::new(format!("line {line}: {msg}"))
    }

    pub(crate) fn parse(line: usize, e: impl fmt::Display) -> Error {
        Error::at(line, e)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.msg)
    }
}

impl std::error::Error for Error {}
