use super::{
    CompareGroup, CompareRegexCache, CompareRow, CompareSpec, compare_row_in_selection_window,
    spanset_matches_row,
};

/// Determines the compare group of a span row.
///
/// The group is `Selection` when the row matches the compare selection spanset
/// and falls inside the optional `[start, end]` sub-window. In every other
/// case the group is `Baseline`.
pub(crate) fn compare_group_for_row(
    row: &CompareRow,
    compare: &CompareSpec,
    regexes: &CompareRegexCache,
    selected_by_plan: Option<bool>,
) -> CompareGroup {
    if compare_row_in_selection_window(row, compare)
        && selected_by_plan.unwrap_or_else(|| spanset_matches_row(&compare.selection, row, regexes))
    {
        CompareGroup::Selection
    } else {
        CompareGroup::Baseline
    }
}
