//! Query-frontend planning helpers for sharded profile queries.

use krabka_pprof::ProfileError;
use krabka_units::{Time, convert::TimeExt, minutes};

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::millis;

    use super::*;

    #[test]
    fn split_inclusive_range_keeps_adjacent_shards_non_overlapping() {
        let shards = split_inclusive_range(0, 10, millis(4)).unwrap();

        assert!(
            shards
                == vec![
                    crate::query::QueryRange::new(0, 3),
                    crate::query::QueryRange::new(4, 7),
                    crate::query::QueryRange::new(8, 10),
                ]
        );
    }

    #[test]
    fn split_inclusive_range_keeps_small_ranges_single_shard() {
        assert!(
            split_inclusive_range(5, 7, millis(10)).unwrap()
                == vec![crate::query::QueryRange::new(5, 7)]
        );
    }

    #[test]
    fn split_inclusive_range_rejects_invalid_inputs() {
        assert!(split_inclusive_range(10, 0, millis(4)).is_err());
        assert!(split_inclusive_range(0, 10, <Time as TimeExt>::ZERO).is_err());
    }
}

mod split_inclusive_range;

pub use split_inclusive_range::split_inclusive_range;

/// Not `Eq`: [`Time`] stores `f64`. Nothing keys a map on this config.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FrontendConfig {
    /// Width of each shard that the query range splits into.
    pub shard_width: Time,
}

impl Default for FrontendConfig {
    fn default() -> Self {
        Self {
            shard_width: minutes(15),
        }
    }
}
