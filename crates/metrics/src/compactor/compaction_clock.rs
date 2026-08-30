
/// Monotonic clock used by the compaction loop to age the accumulation buffer.
///
/// This is an abstraction, so a test can drive flush-by-age deterministically
/// and does not need real wall-clock waits.
pub trait CompactionClock {
    fn now(&self) -> std::time::Instant;
}
