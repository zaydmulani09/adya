//! Histories: what clients asked the database to do, and what came back.
//!
//! The format is Jepsen's: a sequence of operations, each an `invoke` followed
//! (on the same process) by an `ok`, `fail` or `info` completion. An operation's
//! value is a transaction: a list of micro-operations like `["append", 3, 7]`
//! or `["r", 3, [1, 7]]`.

use std::collections::HashMap;
use std::fmt;

use serde_json::Value as Json;

use crate::Error;

/// An interned key or element. Histories mix integers and strings; interning
/// lets the analysis compare plain `u32`s.
pub type Id = u32;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Scalar {
    Int(i64),
    Str(String),
}

impl fmt::Display for Scalar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Scalar::Int(i) => write!(f, "{i}"),
            Scalar::Str(s) => write!(f, "{s:?}"),
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct Interner {
    ids: HashMap<Scalar, Id>,
    items: Vec<Scalar>,
}

impl Interner {
    pub fn intern(&mut self, s: Scalar) -> Id {
        if let Some(&id) = self.ids.get(&s) {
            return id;
        }
        let id = self.items.len() as Id;
        self.items.push(s.clone());
        self.ids.insert(s, id);
        id
    }

    pub fn get(&self, id: Id) -> &Scalar {
        &self.items[id as usize]
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OpType {
    Invoke,
    Ok,
    Fail,
    Info,
}

/// What a read returned. `Nil` is ambiguous on purpose: on an invocation it
/// means "not yet known", on a completion it means the initial (empty) state.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ReadValue {
    Nil,
    List(Vec<Id>),
    Scalar(Id),
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Mop {
    Append { key: Id, value: Id },
    Write { key: Id, value: Id },
    Read { key: Id, value: ReadValue },
}

impl Mop {
    pub fn key(&self) -> Id {
        match *self {
            Mop::Append { key, .. } | Mop::Write { key, .. } | Mop::Read { key, .. } => key,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Op {
    /// The index as written in the history file (or the position, if absent).
    pub index: u64,
    pub kind: OpType,
    /// `None` for non-client processes such as `:nemesis`.
    pub process: Option<i64>,
    pub value: Vec<Mop>,
    /// Nanoseconds, if the history recorded them.
    pub time: Option<i64>,
}

/// A parsed history. `ops[i]` keeps file order; `pair[i]` links an invocation
/// to its completion and back.
#[derive(Clone, Debug, Default)]
pub struct History {
    pub ops: Vec<Op>,
    pub pair: Vec<Option<usize>>,
    pub interner: Interner,
}

impl History {
    /// Parses JSON: either a single array of ops, or one op per line (the
    /// format `elle-cli` reads).
    pub fn from_json(text: &str) -> Result<History, Error> {
        let trimmed = text.trim_start();
        let values: Vec<(usize, Json)> = if trimmed.starts_with('[') {
            let all: Vec<Json> = serde_json::from_str(trimmed).map_err(|e| Error::parse(0, e))?;
            all.into_iter().enumerate().map(|(i, v)| (i + 1, v)).collect()
        } else {
            let mut out = Vec::new();
            for (i, line) in text.lines().enumerate() {
                if line.trim().is_empty() {
                    continue;
                }
                out.push((i + 1, serde_json::from_str(line).map_err(|e| Error::parse(i + 1, e))?));
            }
            out
        };
        let mut builder = Builder::default();
        for (line, v) in values {
            builder.push(line, &v)?;
        }
        Ok(builder.finish())
    }

    /// Parses Jepsen's EDN history format (`history.edn`): one op map per
    /// line, or a single vector of them.
    pub fn from_edn(text: &str) -> Result<History, Error> {
        let mut forms = crate::edn::parse_all(text)?;
        if forms.len() == 1 && forms[0].is_array() {
            forms = forms.pop().unwrap().as_array().cloned().unwrap_or_default();
        }
        let mut builder = Builder::default();
        for (i, v) in forms.iter().enumerate() {
            builder.push(i + 1, v)?;
        }
        Ok(builder.finish())
    }

    /// Parses JSON or EDN, whichever the text looks like.
    pub fn parse(text: &str) -> Result<History, Error> {
        let mut chars = text.chars().filter(|c| !c.is_whitespace() && *c != '[');
        match chars.next() {
            Some('{') if chars.next() == Some('"') => History::from_json(text),
            Some('{') | Some('#') => History::from_edn(text),
            _ => History::from_json(text),
        }
    }

    /// Builds a history from already-structured operations (used by the
    /// workload runner, which records ops as it goes).
    pub fn from_ops(ops: Vec<Op>, interner: Interner) -> History {
        let pair = pair_ops(&ops);
        History { ops, pair, interner }
    }

    pub fn completion(&self, i: usize) -> Option<&Op> {
        self.pair[i].map(|j| &self.ops[j])
    }

    pub fn show(&self, id: Id) -> &Scalar {
        self.interner.get(id)
    }
}

#[derive(Default)]
struct Builder {
    ops: Vec<Op>,
    interner: Interner,
}

impl Builder {
    fn push(&mut self, line: usize, v: &Json) -> Result<(), Error> {
        let obj = v.as_object().ok_or_else(|| Error::at(line, "operation is not an object"))?;
        let kind = match obj.get("type").and_then(Json::as_str) {
            Some("invoke") => OpType::Invoke,
            Some("ok") => OpType::Ok,
            Some("fail") => OpType::Fail,
            Some("info") => OpType::Info,
            other => return Err(Error::at(line, format!("bad op type {other:?}"))),
        };
        // Non-transactional ops (nemesis faults, etc.) carry no txn value.
        if let Some(f) = obj.get("f").and_then(Json::as_str) {
            if f != "txn" {
                return Ok(());
            }
        }
        let process = match obj.get("process") {
            None | Some(Json::Null) => Some(0),
            Some(p) => p.as_i64(),
        };
        if process.is_none() {
            return Ok(());
        }
        let mut value = Vec::new();
        if let Some(mops) = obj.get("value").and_then(Json::as_array) {
            for m in mops {
                value.push(self.mop(line, m)?);
            }
        }
        let index = obj.get("index").and_then(Json::as_u64).unwrap_or(self.ops.len() as u64);
        let time = obj.get("time").and_then(Json::as_i64);
        self.ops.push(Op { index, kind, process, value, time });
        Ok(())
    }

    fn mop(&mut self, line: usize, m: &Json) -> Result<Mop, Error> {
        let parts = m.as_array().filter(|a| a.len() == 3).ok_or_else(|| Error::at(line, "micro-op is not [f k v]"))?;
        let key = self.scalar(line, &parts[1])?;
        let f = parts[0].as_str().unwrap_or("");
        Ok(match f {
            "append" => Mop::Append { key, value: self.scalar(line, &parts[2])? },
            "w" => Mop::Write { key, value: self.scalar(line, &parts[2])? },
            "r" => Mop::Read {
                key,
                value: match &parts[2] {
                    Json::Null => ReadValue::Nil,
                    Json::Array(xs) => {
                        ReadValue::List(xs.iter().map(|x| self.scalar(line, x)).collect::<Result<_, _>>()?)
                    }
                    x => ReadValue::Scalar(self.scalar(line, x)?),
                },
            },
            _ => return Err(Error::at(line, format!("unknown micro-op {:?}", parts[0]))),
        })
    }

    fn scalar(&mut self, line: usize, v: &Json) -> Result<Id, Error> {
        let s = match v {
            Json::Number(n) => Scalar::Int(n.as_i64().ok_or_else(|| Error::at(line, "non-integer number"))?),
            Json::String(s) => Scalar::Str(s.clone()),
            _ => return Err(Error::at(line, format!("expected a key or value, got {v}"))),
        };
        Ok(self.interner.intern(s))
    }

    fn finish(self) -> History {
        History::from_ops(self.ops, self.interner)
    }
}

/// Links each invocation to the next completion on the same process.
fn pair_ops(ops: &[Op]) -> Vec<Option<usize>> {
    let mut pair = vec![None; ops.len()];
    let mut pending: HashMap<i64, usize> = HashMap::new();
    for (i, op) in ops.iter().enumerate() {
        let Some(p) = op.process else { continue };
        if op.kind == OpType::Invoke {
            pending.insert(p, i);
        } else if let Some(inv) = pending.remove(&p) {
            pair[inv] = Some(i);
            pair[i] = Some(inv);
        }
    }
    pair
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_jsonl_and_pairs() {
        let h = History::from_json(
            r#"{"type":"invoke","f":"txn","value":[["append","x",1],["r","x",null]],"process":0,"index":0}
{"type":"invoke","f":"txn","value":[["r","x",null]],"process":1,"index":1}
{"type":"ok","f":"txn","value":[["append","x",1],["r","x",[1]]],"process":0,"index":2}
{"type":"info","f":"start-partition","process":"nemesis","index":3}
{"type":"fail","f":"txn","value":[["r","x",null]],"process":1,"index":4}"#,
        )
        .unwrap();
        assert_eq!(h.ops.len(), 4);
        assert_eq!(h.pair[0], Some(2));
        assert_eq!(h.pair[1], Some(3));
        let x = h.ops[0].value[0].key();
        assert_eq!(h.show(x), &Scalar::Str("x".into()));
        let Mop::Append { value: one, .. } = h.ops[0].value[0] else { panic!() };
        assert_eq!(h.ops[2].value[1], Mop::Read { key: x, value: ReadValue::List(vec![one]) });
    }
}
