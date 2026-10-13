#[cfg(creusot)]
use creusot_std::prelude::*;

/// Inclusive timestamp bounds for one grid shard.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShardTimeRange {
    pub start: i64,
    pub end: i64,
}

/// Returns the inclusive timestamp range for one positive-width grid slot.
///
/// The lower edge clamps when a negative slot runs below `i64::MIN`; the upper
/// edge clamps when the final slot runs above `i64::MAX`.
#[cfg_attr(creusot, requires(width@ >= 1))]
#[cfg_attr(creusot, ensures(result.start@ <= result.end@))]
#[cfg_attr(creusot, ensures(result.end@ - result.start@ <= width@ - 1))]
#[must_use]
#[allow(clippy::manual_unwrap_or)]
pub fn shard_range(slot: i64, width: i64) -> ShardTimeRange {
    let start = match slot.checked_mul(width) {
        Some(start) => start,
        None => i64::MIN,
    };
    let end = start.saturating_add(width - 1);
    ShardTimeRange { start, end }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::{ShardTimeRange, shard_range};

    #[test]
    fn computes_regular_and_clamped_grid_ranges() {
        check!(shard_range(3, 10) == ShardTimeRange { start: 30, end: 39 });
        check!(
            shard_range(-2, 10)
                == ShardTimeRange {
                    start: -20,
                    end: -11
                }
        );
        check!(
            shard_range(i64::MIN, 2)
                == ShardTimeRange {
                    start: i64::MIN,
                    end: i64::MIN + 1
                }
        );
        check!(
            shard_range(i64::MAX, 2)
                == ShardTimeRange {
                    start: i64::MIN,
                    end: i64::MIN + 1
                }
        );
        check!(
            shard_range(1, i64::MAX)
                == ShardTimeRange {
                    start: i64::MAX,
                    end: i64::MAX
                }
        );
    }
}
