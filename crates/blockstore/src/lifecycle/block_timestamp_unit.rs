use super::{Time, TimeExt};

/// The unit a signal's block timestamps count in.
///
/// A block's `min_ts` and `max_ts` are plain `i64` ticks, and the signals do
/// not agree on what a tick is. A retention window is a [`Time`], so something
/// has to say which unit to express it in before the two can be compared. A
/// caller that names the unit here cannot be wrong by a factor of a million,
/// and an `i64` "now" paired with the wrong unit fails to compile rather than
/// expiring the wrong blocks.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum BlockTimestampUnit {
    /// Epoch milliseconds, as the metrics and profiles blocks count.
    ///
    /// A profile row's field is named `timestamp_ns` and holds a millisecond
    /// value, so the field name is not evidence of the unit here.
    Millis,
    /// Epoch nanoseconds, as the traces blocks count.
    Nanos,
}

impl BlockTimestampUnit {
    /// `extent` as a count of ticks in this unit.
    ///
    /// A negative extent is not a window, so it counts as zero ticks. The
    /// callers read zero as "keep forever", which is what an operator who
    /// configured a negative window meant.
    #[must_use]
    pub fn ticks(self, extent: Time) -> i64 {
        let ticks = match self {
            Self::Millis => extent.millis_i64(),
            Self::Nanos => extent.nanos_i64(),
        };
        ticks.max(0)
    }
}
