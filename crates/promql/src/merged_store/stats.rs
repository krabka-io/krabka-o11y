//! TSDB-stat merge helpers for [`super::MergedMetricStore`].

use std::collections::BTreeMap;

use crate::NamedTsdbStat;

mod merge_named_stats;

pub(super) use merge_named_stats::merge_named_stats;

/// Combines the head min-time of two stores with explicit presence flags.
///
/// A store reports emptiness with `None`, not with a `0` min-time, and the
/// caller threads a has-data flag through for this. This function keeps a
/// legitimate `min_time == 0` from a store that does hold samples, and never
/// reads that value as an empty store.
pub(crate) fn min_present_time(left: Option<i64>, right: Option<i64>) -> i64 {
    match left {
        Some(left) => match right {
            Some(right) => left.min(right),
            None => left,
        },
        None => right.unwrap_or_default(),
    }
}
