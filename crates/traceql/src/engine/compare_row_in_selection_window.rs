use super::*;

/// Tells whether the row start time is inside the compare selection window.
///
/// The selection sub-window is optional. With no window, every span that
/// matched the outer spanset is eligible for the selection group.
pub(crate) fn compare_row_in_selection_window(row: &CompareRow, compare: &CompareSpec) -> bool {
    compare.start.is_none_or(|start| row.ts >= start) && compare.end.is_none_or(|end| row.ts <= end)
}
