/// How many blocks a scan probes at once before registering them.
///
/// Each probe is one `head` and one footer read, so the work is latency and
/// not CPU, and doing them one at a time would add a round trip per candidate
/// block to every query. The bound keeps a wide query from opening a
/// connection per block against the object store.
pub(crate) const BLOCK_PROBE_CONCURRENCY: usize = 16;
