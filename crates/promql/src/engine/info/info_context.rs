use super::*;

/// Parsed, store-independent context for `info(v [, data_label_selector])`.
pub(crate) struct InfoContext<'a> {
    pub(crate) data_label_selector: Option<&'a VectorSelector>,
    pub(crate) data_label_matchers: Vec<LabelMatcher>,
    pub(crate) required_data_label_matchers_match_empty: bool,
    pub(crate) selected_data_labels: BTreeSet<String>,
    pub(crate) restrict_data_labels: bool,
}
