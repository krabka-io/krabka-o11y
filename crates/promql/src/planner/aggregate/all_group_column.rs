/// Synthetic per-row grouping column for an empty `PromQL` grouping.
///
/// An empty grouping is `by ()` or no modifier. `GROUP BY` over an empty key set
/// is SQL's "single global group", which emits one row even over an empty input.
/// Prometheus `sum by ()` over zero series yields the empty vector instead. A
/// group by a constant-valued real column makes the group key per-row. An empty
/// input then produces zero groups, which is the Prometheus behaviour, and a
/// non-empty input collapses to exactly one group.
///
/// This module drops the column at assembly, so it never appears in the
/// projected output.
pub(crate) const ALL_GROUP_COLUMN: &str = "__krabka_agg_all__";
