use super::*;

/// The reserved label key that Tempo uses to tag a compare series.
///
/// The tag holds the group, `baseline` or `selection`, or the per-group total,
/// `baseline_total` or `selection_total`. Grafana's exploretraces Comparison
/// view reads this key.
pub(crate) const META_TYPE_KEY: &str = "__meta_type";
