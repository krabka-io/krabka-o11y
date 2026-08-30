use super::*;

pub(crate) fn format_label_selection_set(selections: &LabelSelectionSet) -> String {
    selections
        .selections()
        .iter()
        .map(|selection| {
            let Some(matcher) = selection.matcher() else {
                return selection.name_str().to_string();
            };
            match matcher {
                LabelSelectionMatcher::Equal(value) => {
                    format!("{}={}", selection.name_str(), quote_logql_string(value))
                }
                LabelSelectionMatcher::Regex(pattern) => {
                    format!("{}=~{}", selection.name_str(), quote_logql_string(pattern))
                }
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}
