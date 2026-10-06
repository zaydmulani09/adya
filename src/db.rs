//! Database clients for the workload runner.
//!
//! Every SQL driver uses the same two tables: `adya_lists (k, v)` storing a
//! list as comma-separated text, appended to with an upsert, and
//! `adya_regs (k, v)` for registers. `prepare` recreates them empty. `list`
//! says which table reads go to.

use crate::gen::TxnOp;
use crate::run::{Client, Outcome};
use crate::Error;

/// Isolation level names accepted by the SQL drivers.
#[cfg(any(feature = "postgres", feature = "mysql"))]
pub const LEVELS: [&str; 4] = ["read-uncommitted", "read-committed", "repeatable-read", "serializable"];

#[cfg(any(feature = "postgres", feature = "mysql"))]
fn level_sql(level: &str) -> Result<&'static str, Error> {
    Ok(match level {
        "read-uncommitted" => "READ UNCOMMITTED",
        "read-committed" => "READ COMMITTED",
        "repeatable-read" => "REPEATABLE READ",
        "serializable" => "SERIALIZABLE",
        _ => return Err(Error::new(format!("unknown isolation level {level:?}; expected one of {LEVELS:?}"))),
    })
}

#[cfg(feature = "sqlite")]
pub mod sqlite {
    use super::*;
    use crate::run::parse_list;
    use rusqlite::{params, Connection, ErrorCode, OptionalExtension};

    /// SQLite has one isolation level (serializable); what varies is how a
    /// transaction takes its locks.
    pub struct Sqlite {
        conn: Connection,
        begin: &'static str,
        list: bool,
    }

    pub fn prepare(path: &str) -> Result<(), Error> {
        let c = Connection::open(path).map_err(err)?;
        c.execute_batch(
            "PRAGMA journal_mode=WAL;
             DROP TABLE IF EXISTS adya_lists; DROP TABLE IF EXISTS adya_regs;
             CREATE TABLE adya_lists (k INTEGER PRIMARY KEY, v TEXT NOT NULL);
             CREATE TABLE adya_regs (k INTEGER PRIMARY KEY, v INTEGER NOT NULL);",
        )
        .map_err(err)
    }

    /// `mode` is `deferred`, `immediate` or `exclusive`.
    pub fn connect(path: &str, mode: &str, list: bool) -> Result<Box<dyn Client>, Error> {
        let begin = match mode {
            "deferred" => "BEGIN DEFERRED",
            "immediate" => "BEGIN IMMEDIATE",
            "exclusive" => "BEGIN EXCLUSIVE",
            _ => return Err(Error::new(format!("unknown SQLite transaction mode {mode:?}"))),
        };
        let conn = Connection::open(path).map_err(err)?;
        conn.busy_timeout(std::time::Duration::from_secs(5)).map_err(err)?;
        Ok(Box::new(Sqlite { conn, begin, list }))
    }

    fn err(e: rusqlite::Error) -> Error {
        Error::new(format!("sqlite: {e}"))
    }

    impl Sqlite {
        fn body(&self, ops: &[TxnOp]) -> rusqlite::Result<Vec<Option<Vec<i64>>>> {
            let list = self.list;
            let mut reads = Vec::with_capacity(ops.len());
            for op in ops {
                reads.push(match *op {
                    TxnOp::Append(k, v) => {
                        self.conn.execute(
                            "INSERT INTO adya_lists (k, v) VALUES (?1, ?2)
                             ON CONFLICT (k) DO UPDATE SET v = v || ',' || excluded.v",
                            params![k, v.to_string()],
                        )?;
                        None
                    }
                    TxnOp::Write(k, v) => {
                        self.conn.execute(
                            "INSERT INTO adya_regs (k, v) VALUES (?1, ?2) ON CONFLICT (k) DO UPDATE SET v = excluded.v",
                            params![k, v],
                        )?;
                        None
                    }
                    TxnOp::Read(k) if list => {
                        let v: Option<String> = self
                            .conn
                            .query_row("SELECT v FROM adya_lists WHERE k = ?1", [k], |r| r.get(0))
                            .optional()?;
                        Some(parse_list(v.as_deref()))
                    }
                    TxnOp::Read(k) => {
                        let v: Option<i64> = self
                            .conn
                            .query_row("SELECT v FROM adya_regs WHERE k = ?1", [k], |r| r.get(0))
                            .optional()?;
                        Some(v.into_iter().collect())
                    }
                });
            }
            Ok(reads)
        }
    }

    impl Client for Sqlite {
        fn txn(&mut self, ops: &[TxnOp]) -> Outcome {
            if let Err(e) = self.conn.execute_batch(self.begin) {
                return Outcome::Fail(e.to_string());
            }
            let reads = match self.body(ops) {
                Ok(r) => r,
                Err(e) => {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Outcome::Fail(e.to_string());
                }
            };
            match self.conn.execute_batch("COMMIT") {
                Ok(()) => Outcome::Ok(reads),
                // A busy COMMIT leaves the transaction open; roll it back.
                Err(e) if e.sqlite_error_code() == Some(ErrorCode::DatabaseBusy) => {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    Outcome::Fail(e.to_string())
                }
                Err(e) => Outcome::Info(e.to_string()),
            }
        }
    }
}

#[cfg(feature = "postgres")]
pub mod postgres {
    use super::*;
    use crate::run::parse_list;
    use ::postgres::error::SqlState;
    use ::postgres::{Client as PgClient, NoTls};

    pub struct Postgres {
        conn: PgClient,
        begin: String,
        list: bool,
    }

    pub fn prepare(url: &str) -> Result<(), Error> {
        let mut c = PgClient::connect(url, NoTls).map_err(err)?;
        c.batch_execute(
            "DROP TABLE IF EXISTS adya_lists; DROP TABLE IF EXISTS adya_regs;
             CREATE TABLE adya_lists (k BIGINT PRIMARY KEY, v TEXT NOT NULL);
             CREATE TABLE adya_regs (k BIGINT PRIMARY KEY, v BIGINT NOT NULL);",
        )
        .map_err(err)
    }

    pub fn connect(url: &str, level: &str, list: bool) -> Result<Box<dyn Client>, Error> {
        let begin = format!("BEGIN ISOLATION LEVEL {}", level_sql(level)?);
        let conn = PgClient::connect(url, NoTls).map_err(err)?;
        Ok(Box::new(Postgres { conn, begin, list }))
    }

    fn err(e: ::postgres::Error) -> Error {
        Error::new(format!("postgres: {e}"))
    }

    impl Client for Postgres {
        fn healthy(&mut self) -> bool {
            !self.conn.is_closed()
        }

        fn txn(&mut self, ops: &[TxnOp]) -> Outcome {
            let list = self.list;
            let begin = self.begin.clone();
            let body = |c: &mut PgClient| -> Result<Vec<Option<Vec<i64>>>, ::postgres::Error> {
                c.batch_execute(&begin)?;
                let mut reads = Vec::with_capacity(ops.len());
                for op in ops {
                    reads.push(match *op {
                        TxnOp::Append(k, v) => {
                            c.execute(
                                "INSERT INTO adya_lists AS t (k, v) VALUES ($1, $2)
                                 ON CONFLICT (k) DO UPDATE SET v = t.v || ',' || excluded.v",
                                &[&k, &v.to_string()],
                            )?;
                            None
                        }
                        TxnOp::Write(k, v) => {
                            c.execute(
                                "INSERT INTO adya_regs (k, v) VALUES ($1, $2) ON CONFLICT (k) DO UPDATE SET v = excluded.v",
                                &[&k, &v],
                            )?;
                            None
                        }
                        TxnOp::Read(k) if list => {
                            let row = c.query_opt("SELECT v FROM adya_lists WHERE k = $1", &[&k])?;
                            Some(parse_list(row.as_ref().map(|r| r.get::<_, &str>(0))))
                        }
                        TxnOp::Read(k) => {
                            let row = c.query_opt("SELECT v FROM adya_regs WHERE k = $1", &[&k])?;
                            Some(row.map(|r| r.get::<_, i64>(0)).into_iter().collect())
                        }
                    });
                }
                Ok(reads)
            };
            let reads = match body(&mut self.conn) {
                Ok(r) => r,
                Err(e) => {
                    let _ = self.conn.batch_execute("ROLLBACK");
                    return Outcome::Fail(e.to_string());
                }
            };
            match self.conn.batch_execute("COMMIT") {
                Ok(()) => Outcome::Ok(reads),
                // The server answered: the commit was refused.
                Err(e) if e.code().is_some() => {
                    let definite = matches!(e.code(), Some(c) if *c == SqlState::T_R_SERIALIZATION_FAILURE || *c == SqlState::T_R_DEADLOCK_DETECTED);
                    if definite || e.as_db_error().is_some() {
                        Outcome::Fail(e.to_string())
                    } else {
                        Outcome::Info(e.to_string())
                    }
                }
                // Connection trouble mid-commit: we can't know.
                Err(e) => Outcome::Info(e.to_string()),
            }
        }
    }
}

#[cfg(feature = "mysql")]
pub mod mysql {
    use super::*;
    use crate::run::parse_list;
    use ::mysql::prelude::Queryable;
    use ::mysql::{Conn, Opts};

    pub struct Mysql {
        conn: Conn,
        list: bool,
        /// Set after a client-side (I/O, protocol) error.
        broken: bool,
    }

    fn open(url: &str) -> Result<Conn, Error> {
        let opts = Opts::from_url(url).map_err(|e| Error::new(format!("mysql: {e}")))?;
        Conn::new(opts).map_err(err)
    }

    pub fn prepare(url: &str) -> Result<(), Error> {
        let mut c = open(url)?;
        c.query_drop(
            "DROP TABLE IF EXISTS adya_lists; DROP TABLE IF EXISTS adya_regs;
             CREATE TABLE adya_lists (k BIGINT PRIMARY KEY, v TEXT NOT NULL) ENGINE=InnoDB;
             CREATE TABLE adya_regs (k BIGINT PRIMARY KEY, v BIGINT NOT NULL) ENGINE=InnoDB;",
        )
        .map_err(err)
    }

    pub fn connect(url: &str, level: &str, list: bool) -> Result<Box<dyn Client>, Error> {
        let mut conn = open(url)?;
        conn.query_drop(format!("SET SESSION TRANSACTION ISOLATION LEVEL {}", level_sql(level)?)).map_err(err)?;
        Ok(Box::new(Mysql { conn, list, broken: false }))
    }

    fn err(e: ::mysql::Error) -> Error {
        Error::new(format!("mysql: {e}"))
    }

    impl Client for Mysql {
        fn healthy(&mut self) -> bool {
            !self.broken
        }

        fn txn(&mut self, ops: &[TxnOp]) -> Outcome {
            let list = self.list;
            let body = |c: &mut Conn| -> Result<Vec<Option<Vec<i64>>>, ::mysql::Error> {
                c.query_drop("START TRANSACTION")?;
                let mut reads = Vec::with_capacity(ops.len());
                for op in ops {
                    reads.push(match *op {
                        TxnOp::Append(k, v) => {
                            c.exec_drop(
                                "INSERT INTO adya_lists (k, v) VALUES (?, ?) ON DUPLICATE KEY UPDATE v = CONCAT(v, ',', VALUES(v))",
                                (k, v.to_string()),
                            )?;
                            None
                        }
                        TxnOp::Write(k, v) => {
                            c.exec_drop("INSERT INTO adya_regs (k, v) VALUES (?, ?) ON DUPLICATE KEY UPDATE v = VALUES(v)", (k, v))?;
                            None
                        }
                        TxnOp::Read(k) if list => {
                            let v: Option<String> = c.exec_first("SELECT v FROM adya_lists WHERE k = ?", (k,))?;
                            Some(parse_list(v.as_deref()))
                        }
                        TxnOp::Read(k) => {
                            let v: Option<i64> = c.exec_first("SELECT v FROM adya_regs WHERE k = ?", (k,))?;
                            Some(v.into_iter().collect())
                        }
                    });
                }
                Ok(reads)
            };
            let reads = match body(&mut self.conn) {
                Ok(r) => r,
                Err(e) => {
                    self.broken = !matches!(e, ::mysql::Error::MySqlError(_));
                    let _ = self.conn.query_drop("ROLLBACK");
                    return Outcome::Fail(e.to_string());
                }
            };
            match self.conn.query_drop("COMMIT") {
                Ok(()) => Outcome::Ok(reads),
                Err(::mysql::Error::MySqlError(e)) => Outcome::Fail(e.to_string()),
                Err(e) => Outcome::Info(e.to_string()),
            }
        }
    }
}

/// Talks to any database through a child process speaking JSON lines.
///
/// For each transaction adya writes one line to the child's stdin:
///
/// ```text
/// {"value":[["append",3,7],["r",4,null]]}
/// ```
///
/// and expects one line back: `{"type":"ok","value":[["append",3,7],["r",4,[1,7]]]}`,
/// `{"type":"fail","error":"..."}` if it definitely aborted, or
/// `{"type":"info","error":"..."}` if the outcome is unknown. Register reads
/// return a number or `null`.
pub mod exec {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::process::{Child, ChildStdin, ChildStdout, Stdio};

    use serde_json::{json, Value as Json};

    pub struct Exec {
        child: Child,
        stdin: ChildStdin,
        stdout: BufReader<ChildStdout>,
    }

    impl Drop for Exec {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    pub fn connect(cmd: &str) -> Result<Box<dyn Client>, Error> {
        let mut c = crate::run::shell(cmd);
        let mut child = c
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .map_err(|e| Error::new(format!("could not start {cmd:?}: {e}")))?;
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        Ok(Box::new(Exec { child, stdin, stdout }))
    }

    fn request(ops: &[TxnOp]) -> Json {
        let value: Vec<Json> = ops
            .iter()
            .map(|op| match *op {
                TxnOp::Append(k, v) => json!(["append", k, v]),
                TxnOp::Write(k, v) => json!(["w", k, v]),
                TxnOp::Read(k) => json!(["r", k, null]),
            })
            .collect();
        json!({ "value": value })
    }

    /// Extracts read results from an `ok` response, checking it echoes the
    /// request's micro-ops.
    fn reads(ops: &[TxnOp], resp: &Json) -> Result<Vec<Option<Vec<i64>>>, String> {
        let value = resp["value"].as_array().ok_or("ok response without a value")?;
        if value.len() != ops.len() {
            return Err(format!("expected {} micro-ops back, got {}", ops.len(), value.len()));
        }
        ops.iter()
            .zip(value)
            .map(|(op, m)| match op {
                TxnOp::Read(_) => match &m[2] {
                    Json::Null => Ok(Some(vec![])),
                    Json::Array(xs) => xs
                        .iter()
                        .map(|x| x.as_i64().ok_or("non-integer element"))
                        .collect::<Result<_, _>>()
                        .map(Some)
                        .map_err(String::from),
                    x => x.as_i64().map(|v| Some(vec![v])).ok_or_else(|| format!("bad read result {x}")),
                },
                _ => Ok(None),
            })
            .collect()
    }

    impl Client for Exec {
        fn txn(&mut self, ops: &[TxnOp]) -> Outcome {
            if let Err(e) = writeln!(self.stdin, "{}", request(ops)).and_then(|_| self.stdin.flush()) {
                return Outcome::Fail(format!("write to child: {e}"));
            }
            let mut line = String::new();
            match self.stdout.read_line(&mut line) {
                Ok(0) | Err(_) => return Outcome::Info("child closed its stdout".into()),
                Ok(_) => {}
            }
            let resp: Json = match serde_json::from_str(&line) {
                Ok(j) => j,
                Err(e) => return Outcome::Info(format!("unparseable response {line:?}: {e}")),
            };
            let error = resp["error"].as_str().unwrap_or("").to_string();
            match resp["type"].as_str() {
                Some("ok") => match reads(ops, &resp) {
                    Ok(r) => Outcome::Ok(r),
                    Err(e) => Outcome::Info(e),
                },
                Some("fail") => Outcome::Fail(error),
                _ => Outcome::Info(error),
            }
        }
    }
}
