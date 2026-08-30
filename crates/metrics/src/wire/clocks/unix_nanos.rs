use super::{Deserialize, Serialize, i64, NANOS_PER_MILLI, TimeExt, Time};

/// An instant on the Unix epoch timeline, in nanoseconds.
///
/// A clock reading carries three of these: the host's own reading, the last
/// instant the clock held a valid reference, and the stamp the ingester writes
/// on arrival. They share a primitive and a scale, and swapping the host
/// reading with the ingest stamp would invert the very skew this signal
/// measures. The newtype makes that swap a compile error.
///
/// This is a coordinate and not an extent, so it stays an integer and never
/// becomes a [`Time`].
#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(transparent)]
pub struct UnixNanos(pub(crate) i64);

impl UnixNanos {
    /// The Unix epoch itself.
    pub const EPOCH: Self = Self(0);

    /// Wraps a raw nanosecond count.
    #[must_use]
    pub const fn new(nanos: i64) -> Self {
        Self(nanos)
    }

    /// The instant as a raw nanosecond count.
    #[must_use]
    pub const fn as_i64(self) -> i64 {
        self.0
    }

    /// The instant in epoch milliseconds, which is the unit every metric block
    /// timestamp column uses.
    ///
    /// The division floors, so an instant before the epoch lands on the
    /// millisecond that contains it rather than on the one after it.
    #[must_use]
    pub const fn epoch_millis(self) -> i64 {
        self.0.div_euclid(NANOS_PER_MILLI)
    }

    /// The instant in fractional epoch seconds, for a projected series.
    #[must_use]
    pub fn epoch_secs_f64(self) -> f64 {
        Time::from_nanos(self.0).secs_f64()
    }

    /// The extent from `self` to `later`.
    ///
    /// The subtraction saturates, so a wire value at either end of the `i64`
    /// range yields the widest representable extent rather than wrapping into
    /// a small one of the wrong sign.
    #[must_use]
    pub fn extent_to(self, later: Self) -> Time {
        Time::from_nanos(later.0.saturating_sub(self.0))
    }
}

impl From<i64> for UnixNanos {
    fn from(nanos: i64) -> Self {
        Self(nanos)
    }
}
