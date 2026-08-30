use super::*;

pub(crate) fn parse_vector_group_modifier(query: &str, position: usize) -> Option<(String, usize)> {
    for modifier in ["group_left", "group_right"] {
        if let Some(rest) = query[position..].strip_prefix(modifier) {
            let Some(labels) = rest.strip_prefix('(') else {
                return Some((modifier.to_string(), position + modifier.len()));
            };
            let labels_end = labels.find(')')?;
            let labels = &labels[..labels_end];
            if labels.is_empty() {
                return Some((modifier.to_string(), position + modifier.len() + 2));
            }
            return Some((
                format!("{modifier} ({labels})"),
                position + modifier.len() + 1 + labels_end + 1,
            ));
        }
    }
    None
}
