#[cfg(creusot)]
use creusot_std::prelude::*;

#[cfg(creusot)]
#[logic]
fn retention_cutoff_model(now_ts: i64, retention_ticks: i64) -> Option<Int> {
    pearlite! {
        if retention_ticks@ <= 0 {
            None
        } else if now_ts@ - retention_ticks@ < -9223372036854775807 - 1 {
            Some(-9223372036854775807 - 1)
        } else {
            Some(now_ts@ - retention_ticks@)
        }
    }
}

/// Computes a tenant's retention cutoff.
///
/// A non-positive window keeps data forever. Positive windows saturate at the
/// timestamp floor rather than overflowing.
#[cfg_attr(creusot, ensures(result.deep_model() == retention_cutoff_model(now_ts, retention_ticks)))]
#[cfg_attr(creusot, ensures((result == None) == (retention_ticks@ <= 0)))]
#[must_use]
pub const fn retention_cutoff(now_ts: i64, retention_ticks: i64) -> Option<i64> {
    if retention_ticks <= 0 {
        None
    } else {
        Some(now_ts.saturating_sub(retention_ticks))
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn non_positive_windows_keep_data_forever() {
        check!(retention_cutoff(10, 0) == None);
        check!(retention_cutoff(10, -1) == None);
    }

    #[test]
    fn positive_windows_saturate_at_the_timestamp_floor() {
        check!(retention_cutoff(10, 3) == Some(7));
        check!(retention_cutoff(i64::MIN, 1) == Some(i64::MIN));
    }
}
