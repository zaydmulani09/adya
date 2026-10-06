//! adya: a black-box checker for transactional isolation.
//!
//! Give it a history of transactions a database executed, and it infers the
//! dependency graph between them (Adya's write-write, write-read and
//! read-write edges, plus process and real-time order), then hunts for the
//! cycles that each isolation level forbids.

pub mod history;
pub mod model;

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
