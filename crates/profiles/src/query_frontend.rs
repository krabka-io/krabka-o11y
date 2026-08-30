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

        assert!(shards == vec![(0, 3), (4, 7), (8, 10)]);
    }

    #[test]
    fn split_inclusive_range_keeps_small_ranges_single_shard() {
        assert!(split_inclusive_range(5, 7, millis(10)).unwrap() == vec![(5, 7)]);
    }

    #[test]
    fn split_inclusive_range_rejects_invalid_inputs() {
        assert!(split_inclusive_range(10, 0, millis(4)).is_err());
        assert!(split_inclusive_range(0, 10, <Time as TimeExt>::ZERO).is_err());
    }
}

// === split-modules: generated submodules ===
mod frontend_config;
mod split_inclusive_range;

pub use frontend_config::FrontendConfig;
pub use split_inclusive_range::split_inclusive_range;
