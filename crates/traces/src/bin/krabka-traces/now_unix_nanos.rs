use std::time::{SystemTime, UNIX_EPOCH};

use super::UnixNano;

/// `now` as an epoch-nanosecond instant, which is the unit a traces block
/// counts its bounds in.
///
/// A clock that reads before the Unix epoch, or further ahead than an `i64` of
/// nanoseconds reaches, gives zero. The retention sweep then expires nothing,
/// which is the safe direction for a clock nobody can trust: a "now" that
/// saturated forward would expire every block the deployment holds.
pub(crate) fn now_unix_nanos(now: SystemTime) -> UnixNano {
    UnixNano(
        now.duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|since| i64::try_from(since.as_nanos()).ok())
            .unwrap_or(0),
    )
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    /// A clock the conversion cannot express reads as zero, and zero expires
    /// nothing. Saturating forward instead would expire every block the
    /// deployment holds.
    #[test]
    fn a_clock_outside_the_nanosecond_range_reads_as_zero() {
        check!(now_unix_nanos(UNIX_EPOCH) == UnixNano(0));
        check!(
            now_unix_nanos(UNIX_EPOCH + std::time::Duration::from_secs(1))
                == UnixNano(1_000_000_000)
        );
        check!(
            now_unix_nanos(UNIX_EPOCH - std::time::Duration::from_secs(1)) == UnixNano(0),
            "a clock before the epoch"
        );
        check!(
            now_unix_nanos(UNIX_EPOCH + std::time::Duration::from_secs(1 << 40)) == UnixNano(0),
            "a clock past what an i64 of nanoseconds reaches"
        );
    }
}
