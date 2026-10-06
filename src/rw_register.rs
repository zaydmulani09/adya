//! The rw-register workload: transactions read and blindly overwrite
//! registers.

use crate::check::Analysis;
use crate::history::History;

pub fn analyze(h: &History) -> Analysis<'_> {
    let mut a = Analysis::new(h);
    a.unknown("unsupported-workload", "rw-register analysis is not implemented yet");
    a
}
