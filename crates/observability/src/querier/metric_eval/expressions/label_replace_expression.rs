#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LabelReplaceExpression {
    pub(crate) query: String,
    pub(crate) destination_label: String,
    pub(crate) replacement: String,
    pub(crate) source_label: String,
    pub(crate) pattern: String,
}
