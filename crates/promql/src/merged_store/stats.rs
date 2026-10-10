//! TSDB-stat merge helpers for [`super::MergedMetricStore`].

use std::collections::BTreeMap;

use crate::NamedTsdbStat;

mod merge_named_stats;
mod min_present_time;

pub(super) use merge_named_stats::merge_named_stats;
pub(super) use min_present_time::min_present_time;
