use super::{LabelOpsKind, SortOrder};

/// Maps a `PromQL` function name to its label-rewrite or ordering kind. Returns
/// `None` for any function outside this set.
pub(crate) fn label_ops_kind_from_function_name(name: &str) -> Option<LabelOpsKind> {
    Some(match name {
        "label_replace" => LabelOpsKind::LabelReplace,
        "label_join" => LabelOpsKind::LabelJoin,
        "sort" => LabelOpsKind::Sort(SortOrder::Ascending),
        "sort_desc" => LabelOpsKind::Sort(SortOrder::Descending),
        "sort_by_label" => LabelOpsKind::SortByLabel(SortOrder::Ascending),
        "sort_by_label_desc" => LabelOpsKind::SortByLabel(SortOrder::Descending),
        _ => return None,
    })
}
