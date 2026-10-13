use crate::http::params_format::ParsedValueAt;

pub(crate) fn parse_vector_group_modifier(
    query: &str,
    position: usize,
) -> Option<ParsedValueAt<String>> {
    for modifier in ["group_left", "group_right"] {
        if let Some(rest) = query[position..].strip_prefix(modifier) {
            let Some(labels) = rest.strip_prefix('(') else {
                return Some(ParsedValueAt {
                    value: modifier.to_string(),
                    position: position + modifier.len(),
                });
            };
            let labels_end = labels.find(')')?;
            let labels = &labels[..labels_end];
            if labels.is_empty() {
                return Some(ParsedValueAt {
                    value: modifier.to_string(),
                    position: position + modifier.len() + 2,
                });
            }
            return Some(ParsedValueAt {
                value: format!("{modifier} ({labels})"),
                position: position + modifier.len() + 1 + labels_end + 1,
            });
        }
    }
    None
}
