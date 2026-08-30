
/// Metric name of the clock reading series itself.
///
/// The columnar clock block is the source of truth for a reading. This series
/// names it so the block rows fingerprint, index, and shard exactly as every
/// other series does.
pub const CLOCK_READING_METRIC: &str = "krabka_clock_reading";
