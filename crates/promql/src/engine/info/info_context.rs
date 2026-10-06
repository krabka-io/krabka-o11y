use krabka_blockstore::MatchOp;

use super::{BTreeSet, LabelMatcher, VectorSelector};

/// Parsed, store-independent context for `info(v [, data_label_selector])`.
pub(crate) struct InfoContext<'a> {
    pub(crate) data_label_selector: Option<&'a VectorSelector>,
    pub(crate) data_label_matchers: Vec<LabelMatcher>,
    pub(crate) required_data_label_matchers_match_empty: bool,
    pub(crate) selected_data_labels: BTreeSet<String>,
    pub(crate) restrict_data_labels: bool,
}

impl InfoContext<'_> {
    pub(crate) fn info_name_matchers(&self) -> Vec<LabelMatcher> {
        let mut matchers = self
            .data_label_matchers
            .iter()
            .filter(|matcher| matcher.name == "__name__")
            .cloned()
            .collect::<Vec<_>>();
        if matchers.is_empty() {
            matchers.push(LabelMatcher::new("__name__", MatchOp::Eq, "target_info"));
        } else if !matchers
            .iter()
            .any(|matcher| matches!(matcher.op, MatchOp::Eq | MatchOp::Re))
        {
            matchers.push(LabelMatcher::new("__name__", MatchOp::Re, ".+_info"));
        }
        matchers
    }
}
