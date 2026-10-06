//! Transaction dependency graphs and the cycle search over them.
//!
//! Vertices are committed (or possibly-committed) transactions. Each edge
//! carries a bitset of relationships; when one edge has several, the
//! strongest-evidence one (`ww` > `wr` > `rw` > `process` > `realtime`) is the
//! edge's *type*, which is what anomaly classification uses.

use std::collections::HashMap;
use std::time::Instant;

pub const WW: u8 = 1;
pub const WR: u8 = 2;
pub const RW: u8 = 4;
pub const PROCESS: u8 = 8;
pub const REALTIME: u8 = 16;

/// The edge's type: its lowest set bit, given the priority order above.
pub fn edge_type(rels: u8) -> u8 {
    rels & rels.wrapping_neg()
}

pub fn rel_name(rel: u8) -> &'static str {
    match rel {
        WW => "ww",
        WR => "wr",
        RW => "rw",
        PROCESS => "process",
        REALTIME => "realtime",
        _ => "?",
    }
}

#[derive(Default)]
pub struct GraphBuilder {
    edges: HashMap<(u32, u32), u8>,
}

impl GraphBuilder {
    pub fn link(&mut self, a: u32, b: u32, rel: u8) {
        if a != b {
            *self.edges.entry((a, b)).or_insert(0) |= rel;
        }
    }

    pub fn finish(self, n: usize) -> Graph {
        let mut adj = vec![Vec::new(); n];
        for ((a, b), rels) in self.edges {
            adj[a as usize].push((b, rels));
        }
        for out in &mut adj {
            out.sort_unstable();
        }
        Graph { adj }
    }
}

pub struct Graph {
    /// `adj[a]` lists `(b, rels)`, sorted by `b`.
    pub adj: Vec<Vec<(u32, u8)>>,
}

impl Graph {
    pub fn len(&self) -> usize {
        self.adj.len()
    }

    pub fn is_empty(&self) -> bool {
        self.adj.is_empty()
    }

    pub fn rels(&self, a: u32, b: u32) -> u8 {
        let out = &self.adj[a as usize];
        out.binary_search_by_key(&b, |e| e.0).map_or(0, |i| out[i].1)
    }

    /// Strongly connected components of the subgraph of edges for which
    /// `keep(rels)` holds, restricted to `nodes`. Only nontrivial components
    /// (more than one vertex; there are no self-edges) are returned, smallest
    /// first.
    pub fn sccs(&self, nodes: &[u32], keep: impl Fn(u8) -> bool) -> Vec<Vec<u32>> {
        // Iterative Tarjan over a local index space.
        let local: HashMap<u32, u32> = nodes.iter().enumerate().map(|(i, &v)| (v, i as u32)).collect();
        let n = nodes.len();
        const UNSEEN: u32 = u32::MAX;
        let mut index = vec![UNSEEN; n];
        let mut low = vec![0u32; n];
        let mut on_stack = vec![false; n];
        let mut stack = Vec::new();
        let mut out = Vec::new();
        let mut next = 0u32;
        for root in 0..n {
            if index[root] != UNSEEN {
                continue;
            }
            // Call stack frames: (vertex, position in its adjacency list).
            let mut calls = vec![(root, 0usize)];
            index[root] = next;
            low[root] = next;
            next += 1;
            stack.push(root);
            on_stack[root] = true;
            while let Some(&(v, mut pos)) = calls.last() {
                let edges = &self.adj[nodes[v] as usize];
                let mut child = None;
                while pos < edges.len() {
                    let (w, rels) = edges[pos];
                    pos += 1;
                    if !keep(rels) {
                        continue;
                    }
                    let Some(&w) = local.get(&w) else { continue };
                    let w = w as usize;
                    if index[w] == UNSEEN {
                        child = Some(w);
                        break;
                    } else if on_stack[w] {
                        low[v] = low[v].min(index[w]);
                    }
                }
                calls.last_mut().unwrap().1 = pos;
                if let Some(w) = child {
                    index[w] = next;
                    low[w] = next;
                    next += 1;
                    stack.push(w);
                    on_stack[w] = true;
                    calls.push((w, 0));
                    continue;
                }
                calls.pop();
                if let Some(&(parent, _)) = calls.last() {
                    low[parent] = low[parent].min(low[v]);
                }
                if low[v] == index[v] {
                    let mut comp = Vec::new();
                    loop {
                        let w = stack.pop().unwrap();
                        on_stack[w] = false;
                        comp.push(nodes[w]);
                        if w == v {
                            break;
                        }
                    }
                    if comp.len() > 1 {
                        comp.sort_unstable();
                        out.push(comp);
                    }
                }
            }
        }
        out.sort_by_key(Vec::len);
        out
    }
}

/// How many read-write (anti-dependency) edges a cycle may contain.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RwMode {
    None,
    /// Exactly one.
    Single,
    /// At least two, never two in a row (including around the wrap).
    Nonadjacent,
    /// At least two, with some pair in a row.
    Adjacent,
}

/// A family of cycles to look for, in the style of Adya's phenomena.
#[derive(Clone, Copy, Debug)]
pub struct CycleSpec {
    pub name: &'static str,
    /// Edge types usable besides `rw`.
    pub allowed: u8,
    pub rw: RwMode,
    pub need_wr: bool,
    /// At least one edge of these types (process / realtime variants).
    pub need: u8,
}

/// One step of a cycle: from `from`, along an edge of type `rel`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Step {
    pub from: u32,
    pub rel: u8,
}

/// Result of a bounded search.
pub enum Search {
    Found(Vec<Step>),
    NotFound,
    TimedOut,
}

// Product-state flags tracked along a path.
const COUNT_MASK: u8 = 0b11; // rw edges seen, saturating at 2
const LAST_RW: u8 = 1 << 2;
const FIRST_RW: u8 = 1 << 3;
const ADJ: u8 = 1 << 4;
const SAW_WR: u8 = 1 << 5;
const SAW_NEED: u8 = 1 << 6;
const STATES: usize = 128;

impl CycleSpec {
    fn usable(&self, ty: u8) -> bool {
        ty & self.allowed != 0 || (ty == RW && self.rw != RwMode::None)
    }

    /// Advances path flags along an edge of type `ty`, or rejects the edge.
    fn step(&self, flags: u8, ty: u8, first: bool) -> Option<u8> {
        let mut f = flags;
        if ty == RW {
            let count = f & COUNT_MASK;
            match self.rw {
                RwMode::None => return None,
                RwMode::Single if count >= 1 => return None,
                RwMode::Nonadjacent if f & LAST_RW != 0 => return None,
                _ => {}
            }
            if f & LAST_RW != 0 {
                f |= ADJ;
            }
            f = (f & !COUNT_MASK) | (count + 1).min(2);
            f |= LAST_RW;
            if first {
                f |= FIRST_RW;
            }
        } else {
            f &= !LAST_RW;
        }
        if ty == WR {
            f |= SAW_WR;
        }
        if ty & self.need != 0 {
            f |= SAW_NEED;
        }
        Some(f)
    }

    fn accepts(&self, f: u8) -> bool {
        let count = f & COUNT_MASK;
        let adj = f & ADJ != 0 || (f & LAST_RW != 0 && f & FIRST_RW != 0);
        let rw_ok = match self.rw {
            RwMode::None => count == 0,
            RwMode::Single => count == 1,
            RwMode::Nonadjacent => count == 2 && !adj,
            RwMode::Adjacent => count == 2 && adj,
        };
        rw_ok && (!self.need_wr || f & SAW_WR != 0) && (self.need == 0 || f & SAW_NEED != 0)
    }

    /// The edge type every matching cycle can be rotated to start with.
    fn anchor(&self) -> Option<u8> {
        if self.rw != RwMode::None {
            Some(RW)
        } else if self.need_wr {
            Some(WR)
        } else {
            None
        }
    }
}

/// Searches `scc` for a cycle matching `spec`: a shortest one from the first
/// start vertex that has any. Gives up after `deadline`.
pub fn find_cycle(g: &Graph, scc: &[u32], spec: &CycleSpec, deadline: Instant) -> Search {
    let local: HashMap<u32, u32> = scc.iter().enumerate().map(|(i, &v)| (v, i as u32)).collect();
    let n = scc.len();
    // ponytail: O(|scc| * 128) state arrays; fine to ~10^5-vertex SCCs.
    let mut stamp = vec![u32::MAX; n * STATES];
    let mut parent = vec![u32::MAX; n * STATES];
    let mut via = vec![0u8; n * STATES];
    let anchor = spec.anchor();
    let mut queue = std::collections::VecDeque::new();

    for (start, &sv) in scc.iter().enumerate() {
        if start % 64 == 0 && Instant::now() > deadline {
            return Search::TimedOut;
        }
        let gen = start as u32;
        queue.clear();
        // Seed with the first edge, which must be of the anchor type if any.
        for &(w, rels) in &g.adj[sv as usize] {
            let ty = edge_type(rels);
            if !spec.usable(ty) || anchor.is_some_and(|a| a != ty) {
                continue;
            }
            let Some(&w) = local.get(&w) else { continue };
            let Some(f) = spec.step(0, ty, true) else { continue };
            if w as usize == start {
                continue;
            }
            let s = w as usize * STATES + f as usize;
            if stamp[s] != gen {
                stamp[s] = gen;
                parent[s] = u32::MAX; // came straight from start
                via[s] = ty;
                queue.push_back(s);
            }
        }
        while let Some(s) = queue.pop_front() {
            let (v, f) = (s / STATES, (s % STATES) as u8);
            for &(w, rels) in &g.adj[scc[v] as usize] {
                let ty = edge_type(rels);
                if !spec.usable(ty) {
                    continue;
                }
                let Some(&w) = local.get(&w) else { continue };
                let Some(f2) = spec.step(f, ty, false) else { continue };
                if w as usize == start {
                    if spec.accepts(f2) {
                        return Search::Found(unwind(scc, start, s, ty, &parent, &via));
                    }
                    continue;
                }
                let s2 = w as usize * STATES + f2 as usize;
                if stamp[s2] != gen {
                    stamp[s2] = gen;
                    parent[s2] = s as u32;
                    via[s2] = ty;
                    queue.push_back(s2);
                }
            }
        }
    }
    Search::NotFound
}

/// Some shortest cycle through `scc[0]`, ignoring edge types. Every
/// nontrivial SCC has one; this is the fallback when a typed search times out.
pub fn any_cycle(g: &Graph, scc: &[u32]) -> Vec<Step> {
    let local: HashMap<u32, usize> = scc.iter().enumerate().map(|(i, &v)| (v, i)).collect();
    let mut parent: Vec<Option<(usize, u8)>> = vec![None; scc.len()];
    let mut queue = std::collections::VecDeque::from([0usize]);
    while let Some(v) = queue.pop_front() {
        for &(w, rels) in &g.adj[scc[v] as usize] {
            let Some(&w) = local.get(&w) else { continue };
            if w == 0 {
                let mut steps = vec![Step { from: scc[v], rel: edge_type(rels) }];
                let mut x = v;
                while let Some((p, rel)) = parent[x] {
                    steps.push(Step { from: scc[p], rel });
                    x = p;
                }
                steps.reverse();
                return steps;
            }
            if parent[w].is_none() {
                parent[w] = Some((v, edge_type(rels)));
                queue.push_back(w);
            }
        }
    }
    Vec::new()
}

fn unwind(scc: &[u32], start: usize, last: usize, closing: u8, parent: &[u32], via: &[u8]) -> Vec<Step> {
    // Walk back from the last state; each state's vertex was entered via `via`.
    let mut rev = vec![Step { from: scc[last / STATES], rel: closing }];
    let mut s = last;
    loop {
        let p = parent[s];
        let from = if p == u32::MAX { scc[start] } else { scc[p as usize / STATES] };
        rev.push(Step { from, rel: via[s] });
        if p == u32::MAX {
            break;
        }
        s = p as usize;
    }
    rev.reverse();
    rev
}

/// Classifies a cycle the way Adya (and Elle) name them.
pub fn classify(steps: &[Step]) -> String {
    let count = |r: u8| steps.iter().filter(|s| s.rel == r).count();
    let rw = count(RW);
    let adjacent = (0..steps.len()).any(|i| steps[i].rel == RW && steps[(i + 1) % steps.len()].rel == RW);
    let base = if rw == 1 {
        "G-single-item"
    } else if rw > 1 {
        if adjacent {
            "G2-item"
        } else {
            "G-nonadjacent-item"
        }
    } else if count(WR) > 0 {
        "G1c"
    } else {
        "G0"
    };
    let suffix = if count(REALTIME) > 0 {
        "-realtime"
    } else if count(PROCESS) > 0 {
        "-process"
    } else {
        ""
    };
    format!("{base}{suffix}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn graph(n: usize, edges: &[(u32, u32, u8)]) -> Graph {
        let mut b = GraphBuilder::default();
        for &(a, c, r) in edges {
            b.link(a, c, r);
        }
        b.finish(n)
    }

    fn spec(rw: RwMode, allowed: u8, need_wr: bool, need: u8) -> CycleSpec {
        CycleSpec { name: "t", allowed, rw, need_wr, need }
    }

    fn find(g: &Graph, s: &CycleSpec) -> Option<Vec<Step>> {
        let nodes: Vec<u32> = (0..g.len() as u32).collect();
        let deadline = Instant::now() + Duration::from_secs(5);
        for scc in g.sccs(&nodes, |r| edge_type(r) == RW || edge_type(r) & (s.allowed) != 0) {
            if let Search::Found(c) = find_cycle(g, &scc, s, deadline) {
                return Some(c);
            }
        }
        None
    }

    #[test]
    fn scc_basics() {
        let g = graph(5, &[(0, 1, WW), (1, 2, WW), (2, 0, WR), (3, 4, WW)]);
        let all: Vec<u32> = (0..5).collect();
        assert_eq!(g.sccs(&all, |_| true), vec![vec![0, 1, 2]]);
        assert!(g.sccs(&all, |r| r == WW).is_empty());
    }

    #[test]
    fn write_skew_is_g2_not_single() {
        // T0 -rw-> T1 -rw-> T0: classic write skew.
        let g = graph(2, &[(0, 1, RW), (1, 0, RW)]);
        assert!(find(&g, &spec(RwMode::Single, WW | WR, false, 0)).is_none());
        let c = find(&g, &spec(RwMode::Adjacent, WW | WR, false, 0)).unwrap();
        assert_eq!(classify(&c), "G2-item");
    }

    #[test]
    fn g_single_and_nonadjacent() {
        // 0 -wr-> 1 -rw-> 0 : G-single
        let g = graph(2, &[(0, 1, WR), (1, 0, RW)]);
        let c = find(&g, &spec(RwMode::Single, WW | WR, false, 0)).unwrap();
        assert_eq!(classify(&c), "G-single-item");
        // 0 -rw-> 1 -ww-> 2 -rw-> 3 -wr-> 0 : two rws, never adjacent
        let g = graph(4, &[(0, 1, RW), (1, 2, WW), (2, 3, RW), (3, 0, WR)]);
        assert!(find(&g, &spec(RwMode::Adjacent, WW | WR, false, 0)).is_none());
        let c = find(&g, &spec(RwMode::Nonadjacent, WW | WR, false, 0)).unwrap();
        assert_eq!(classify(&c), "G-nonadjacent-item");
        assert_eq!(c.len(), 4);
    }

    #[test]
    fn realtime_variant_needs_realtime_edge() {
        let g = graph(2, &[(0, 1, WW), (1, 0, REALTIME)]);
        assert!(find(&g, &spec(RwMode::None, WW, false, 0)).is_none());
        let c = find(&g, &spec(RwMode::None, WW | REALTIME, false, REALTIME)).unwrap();
        assert_eq!(classify(&c), "G0-realtime");
        // An edge that is both ww and realtime counts as ww.
        let g = graph(2, &[(0, 1, WW | REALTIME), (1, 0, WW)]);
        assert!(find(&g, &spec(RwMode::None, WW | REALTIME, false, REALTIME)).is_none());
    }

    #[test]
    fn g1c_requires_wr() {
        let g = graph(3, &[(0, 1, WW), (1, 2, WR), (2, 0, WW)]);
        let c = find(&g, &spec(RwMode::None, WW | WR, true, 0)).unwrap();
        assert_eq!(classify(&c), "G1c");
        assert_eq!(c.len(), 3);
    }
}
