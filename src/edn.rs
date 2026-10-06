//! Just enough EDN to read Jepsen's `history.edn`: maps, vectors, lists,
//! sets, keywords, symbols, strings, numbers, `nil`, booleans, tagged
//! literals (`#jepsen.history.Op{...}`) and `#_` discards. Values are turned
//! into JSON so the history loader has one input path; keywords become
//! strings without the colon.

use serde_json::{Map, Number, Value as Json};

use crate::Error;

pub fn parse_all(text: &str) -> Result<Vec<Json>, Error> {
    let mut p = Parser { s: text.as_bytes(), i: 0 };
    let mut out = Vec::new();
    loop {
        p.skip();
        if p.i >= p.s.len() {
            return Ok(out);
        }
        if let Some(v) = p.form()? {
            out.push(v);
        }
    }
}

struct Parser<'a> {
    s: &'a [u8],
    i: usize,
}

impl Parser<'_> {
    fn err(&self, msg: &str) -> Error {
        let line = self.s[..self.i.min(self.s.len())].iter().filter(|&&b| b == b'\n').count() + 1;
        Error::at(line, format!("edn: {msg}"))
    }

    fn peek(&self) -> Option<u8> {
        self.s.get(self.i).copied()
    }

    /// Skips whitespace, commas and `;` comments.
    fn skip(&mut self) {
        while let Some(c) = self.peek() {
            if c.is_ascii_whitespace() || c == b',' {
                self.i += 1;
            } else if c == b';' {
                while self.peek().is_some_and(|c| c != b'\n') {
                    self.i += 1;
                }
            } else {
                break;
            }
        }
    }

    /// Reads one form; `None` for a discarded (`#_`) one.
    fn form(&mut self) -> Result<Option<Json>, Error> {
        self.skip();
        let c = self.peek().ok_or_else(|| self.err("unexpected end of input"))?;
        Ok(Some(match c {
            b'{' => {
                self.i += 1;
                let items = self.seq(b'}')?;
                if items.len() % 2 != 0 {
                    return Err(self.err("map with an odd number of forms"));
                }
                let mut m = Map::new();
                for kv in items.chunks(2) {
                    let k = match &kv[0] {
                        Json::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    m.insert(k, kv[1].clone());
                }
                Json::Object(m)
            }
            b'[' => {
                self.i += 1;
                Json::Array(self.seq(b']')?)
            }
            b'(' => {
                self.i += 1;
                Json::Array(self.seq(b')')?)
            }
            b'"' => self.string()?,
            b'#' => {
                self.i += 1;
                match self.peek() {
                    Some(b'{') => {
                        self.i += 1;
                        Json::Array(self.seq(b'}')?)
                    }
                    Some(b'_') => {
                        self.i += 1;
                        self.form()?;
                        return Ok(None);
                    }
                    // A tagged literal: drop the tag, keep the value.
                    _ => {
                        self.token();
                        return self.form();
                    }
                }
            }
            _ => {
                let t = self.token();
                if t.is_empty() {
                    return Err(self.err(&format!("unexpected character {:?}", c as char)));
                }
                atom(t)
            }
        }))
    }

    fn seq(&mut self, close: u8) -> Result<Vec<Json>, Error> {
        let mut out = Vec::new();
        loop {
            self.skip();
            match self.peek() {
                None => return Err(self.err("unclosed collection")),
                Some(c) if c == close => {
                    self.i += 1;
                    return Ok(out);
                }
                _ => {
                    if let Some(v) = self.form()? {
                        out.push(v);
                    }
                }
            }
        }
    }

    fn token(&mut self) -> &str {
        let start = self.i;
        while let Some(c) = self.peek() {
            if c.is_ascii_whitespace() || b",;()[]{}\"".contains(&c) {
                break;
            }
            self.i += 1;
        }
        std::str::from_utf8(&self.s[start..self.i]).unwrap_or("")
    }

    fn string(&mut self) -> Result<Json, Error> {
        self.i += 1;
        let mut out = Vec::new();
        loop {
            match self.peek() {
                None => return Err(self.err("unterminated string")),
                Some(b'"') => {
                    self.i += 1;
                    return Ok(Json::String(String::from_utf8_lossy(&out).into_owned()));
                }
                Some(b'\\') => {
                    self.i += 1;
                    let c = self.peek().ok_or_else(|| self.err("unterminated string"))?;
                    out.push(match c {
                        b'n' => b'\n',
                        b't' => b'\t',
                        b'r' => b'\r',
                        other => other,
                    });
                    self.i += 1;
                }
                Some(c) => {
                    out.push(c);
                    self.i += 1;
                }
            }
        }
    }
}

fn atom(t: &str) -> Json {
    match t {
        "nil" => return Json::Null,
        "true" => return Json::Bool(true),
        "false" => return Json::Bool(false),
        _ => {}
    }
    if let Some(k) = t.strip_prefix(':') {
        return Json::String(k.to_string());
    }
    let num = t.trim_end_matches('N').trim_end_matches('M');
    if let Ok(i) = num.parse::<i64>() {
        return Json::Number(i.into());
    }
    if let Some(n) = num.parse::<f64>().ok().and_then(Number::from_f64) {
        return Json::Number(n);
    }
    Json::String(t.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jepsen_ops() {
        let v = parse_all(
            "{:type :invoke, :f :txn, :value [[:append 9 1] [:r 8 nil]], :time 12, :process 0, :index 0}\n\
             #jepsen.history.Op{:index 1, :type :info, :process :nemesis, :f :start, :value nil} ; comment\n\
             #_ {:ignored true} {:a #{1 2} :b \"x\\\"y\" :c -3.5}",
        )
        .unwrap();
        assert_eq!(v.len(), 3);
        assert_eq!(v[0]["type"], "invoke");
        assert_eq!(v[0]["value"][0], serde_json::json!(["append", 9, 1]));
        assert_eq!(v[0]["value"][1][2], Json::Null);
        assert_eq!(v[1]["process"], "nemesis");
        assert_eq!(v[2]["b"], "x\"y");
        assert_eq!(v[2]["a"].as_array().unwrap().len(), 2);
    }
}
