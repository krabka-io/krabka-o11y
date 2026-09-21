#[cfg(creusot)]
use creusot_std::prelude::*;

/// Returns the inclusive timestamp range for one positive-width grid slot.
///
/// The lower edge clamps when a negative slot runs below `i64::MIN`; the upper
/// edge clamps when the final slot runs above `i64::MAX`.
#[cfg_attr(creusot, requires(width@ >= 1))]
#[cfg_attr(creusot, ensures(result.0@ <= result.1@))]
#[cfg_attr(creusot, ensures(result.1@ - result.0@ <= width@ - 1))]
#[must_use]
pub fn shard_range(slot: i64, width: i64) -> (i64, i64) {
    let start = slot.checked_mul(width).unwrap_or(i64::MIN);
    let end = start.saturating_add(width - 1);
    (start, end)
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::shard_range;

    #[test]
    fn computes_regular_and_clamped_grid_ranges() {
        check!(shard_range(3, 10) == (30, 39));
        check!(shard_range(-2, 10) == (-20, -11));
        check!(shard_range(i64::MIN, 2) == (i64::MIN, i64::MIN + 1));
        check!(shard_range(i64::MAX, 2) == (i64::MIN, i64::MIN + 1));
        check!(shard_range(1, i64::MAX) == (i64::MAX, i64::MAX));
    }
}
