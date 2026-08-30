use super::SortOrder;

/// The label-rewrite and ordering functions that the operator-path
/// `PromqlEngine::plan_label_ops_call` handles.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LabelOpsKind {
    LabelReplace,
    LabelJoin,
    Sort(SortOrder),
    SortByLabel(SortOrder),
}
