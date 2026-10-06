//! Random transaction workloads in the style of Elle's generators: a small
//! pool of hot keys, short transactions mixing reads and writes, unique
//! values per key, and keys retired after enough writes so reads stay short.

use std::collections::HashMap;

/// Tiny deterministic PRNG (splitmix64); enough for workload generation.
#[derive(Clone, Debug)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed ^ 0x9E37_79B9_7F4A_7C15)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n`.
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n.max(1)
    }

    /// True with probability `p`.
    pub fn chance(&mut self, p: f64) -> bool {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64 <= p
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    ListAppend,
    RwRegister,
}

/// One micro-operation to perform. Values are unique per key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxnOp {
    Append(i64, i64),
    Write(i64, i64),
    Read(i64),
}

#[derive(Clone, Debug)]
pub struct GenOpts {
    pub kind: Kind,
    /// Keys in play at any time.
    pub key_count: usize,
    pub min_txn_len: usize,
    pub max_txn_len: usize,
    /// After this many writes a key is retired for a fresh one.
    pub max_writes_per_key: u64,
}

impl Default for GenOpts {
    fn default() -> GenOpts {
        GenOpts { kind: Kind::ListAppend, key_count: 8, min_txn_len: 1, max_txn_len: 4, max_writes_per_key: 32 }
    }
}

pub struct Gen {
    opts: GenOpts,
    rng: Rng,
    active: Vec<i64>,
    next_key: i64,
    written: HashMap<i64, u64>,
}

impl Gen {
    pub fn new(opts: GenOpts, seed: u64) -> Gen {
        let active = (0..opts.key_count as i64).collect();
        Gen { next_key: opts.key_count as i64, opts, rng: Rng::new(seed), active, written: HashMap::new() }
    }

    /// Picks a key, favoring the most recently added ones (each key is about
    /// twice as likely as the one before it).
    fn key(&mut self) -> usize {
        let n = self.active.len();
        let mut i = n - 1;
        while i > 0 && self.rng.chance(0.5) {
            i -= 1;
        }
        i
    }

    pub fn txn(&mut self) -> Vec<TxnOp> {
        let span = (self.opts.max_txn_len - self.opts.min_txn_len + 1) as u64;
        let len = self.opts.min_txn_len + self.rng.below(span) as usize;
        let mut txn = Vec::with_capacity(len);
        while txn.len() < len {
            let i = self.key();
            let k = self.active[i];
            if self.rng.chance(0.5) {
                txn.push(TxnOp::Read(k));
                continue;
            }
            let n = self.written.entry(k).or_insert(0);
            if *n >= self.opts.max_writes_per_key {
                self.active[i] = self.next_key;
                self.next_key += 1;
                continue;
            }
            *n += 1;
            let v = *n as i64;
            txn.push(match self.opts.kind {
                Kind::ListAppend => TxnOp::Append(k, v),
                Kind::RwRegister => TxnOp::Write(k, v),
            });
        }
        txn
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_are_unique_per_key_and_keys_rotate() {
        let mut g = Gen::new(GenOpts { max_writes_per_key: 3, ..GenOpts::default() }, 7);
        let mut seen = std::collections::HashSet::new();
        let mut keys = std::collections::HashSet::new();
        for _ in 0..500 {
            for op in g.txn() {
                if let TxnOp::Append(k, v) = op {
                    assert!(seen.insert((k, v)));
                    assert!(v <= 3);
                    keys.insert(k);
                }
            }
        }
        assert!(keys.len() > 8, "keys should rotate");
    }
}
