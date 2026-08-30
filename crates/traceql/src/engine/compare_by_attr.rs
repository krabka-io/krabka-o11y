use super::{BTreeMap, CompareGroup};

/// `attribute → value → group → per-bucket counts`.
///
/// `build_compare_series` regroups the flat counts into this shape. The shape
/// lets the code choose one shared value set per attribute across both groups.
/// Grafana shows the baseline distribution and the selection distribution side
/// by side, so both must cover the same values.
pub(crate) type CompareByAttr =
    BTreeMap<String, BTreeMap<String, BTreeMap<CompareGroup, Vec<u64>>>>;
