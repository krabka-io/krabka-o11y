/// Stacktrace id reserved for samples with no frames.
///
/// A goroutine profile, for example, holds an occasional stackless record. This
/// id stays distinct from node `0`, the root of the first real stacktrace.
/// Stackless samples then resolve to no frames, and they do not take the root
/// of another stack. Flamegraph totals exclude these samples, but series sums
/// still count them.
pub const EMPTY_STACKTRACE_ID: u32 = u32::MAX;
