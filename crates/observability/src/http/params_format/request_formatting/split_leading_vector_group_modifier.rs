use super::*;

pub(crate) fn split_leading_vector_group_modifier(query: &str) -> (Option<String>, &str) {
    let query = query.trim_start();
    for modifier in ["group_left", "group_right"] {
        if let Some(rest) = query.strip_prefix(modifier) {
            let rest = rest.trim_start();
            let Some(labels) = rest.strip_prefix('(') else {
                return (Some(modifier.to_string()), rest);
            };
            let Some(labels_end) = labels.find(')') else {
                return (None, query);
            };
            let labels_text = &labels[..labels_end];
            let modifier_text = if labels_text.is_empty() {
                modifier.to_string()
            } else {
                format!("{modifier} ({labels_text})")
            };
            return (Some(modifier_text), &labels[labels_end + 1..]);
        }
    }
    (None, query)
}
