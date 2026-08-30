use super::{Time, TimeExt};

/// The extent between two epoch-millisecond instants, clamped at zero so a
/// reversed range reads as "no span" and not as a negative one.
pub(crate) fn extent_between(start_ms: i64, end_ms: i64) -> Time {
    Time::from_millis(end_ms.saturating_sub(start_ms).max(0))
}
