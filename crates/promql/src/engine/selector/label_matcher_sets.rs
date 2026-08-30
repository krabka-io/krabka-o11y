use super::*;

pub(crate) fn label_matcher_sets(selector: &VectorSelector) -> Vec<Vec<LabelMatcher>> {
    if selector.matchers.or_matchers.is_empty() {
        return vec![build_label_matchers(
            selector.name.as_deref(),
            &selector.matchers.matchers,
        )];
    }

    let mut out = Vec::new();
    for matchers in &selector.matchers.or_matchers {
        out.push(build_label_matchers(selector.name.as_deref(), matchers));
    }
    out
}
