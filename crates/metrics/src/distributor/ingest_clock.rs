use super::Instant;

/// Monotonic clock that stamps series and stream activity on the ingest path.
///
/// This is an abstraction, so a test can drive an idle timeout deterministically
/// and does not need a real wall-clock wait.
pub trait IngestClock: Send + Sync + std::fmt::Debug {
    fn now(&self) -> Instant;
}
