/// Scale between the epoch-nanosecond timestamps that the WAL carries and the
/// epoch-millisecond timestamps that index blocks.
///
/// This is instant arithmetic and not an extent, so it stays exact integer
/// division. An absolute nanosecond timestamp is about 1.8e18 and cannot
/// round-trip through the `f64` seconds that a `Time` stores.
pub(crate) const NANOS_PER_MILLI: i64 = 1_000_000;
