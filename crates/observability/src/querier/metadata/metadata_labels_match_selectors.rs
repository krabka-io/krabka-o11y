use super::Labels;

pub(crate) fn metadata_labels_match_selectors(
    labels: &Labels,
    selectors: &[krabka_logql::StreamQuery],
) -> bool {
    if selectors.is_empty() {
        return true;
    }

    selectors.iter().any(|selector| {
        selector
            .matchers
            .iter()
            .all(|matcher| matcher.matches(labels))
    })
}
