use super::*;

pub(crate) fn split_leading_vector_matching_modifier(query: &str) -> Option<(String, &str)> {
    let query = query.trim_start();
    for modifier in ["on", "ignoring"] {
        if let Some(rest) = query.strip_prefix(modifier) {
            let labels = rest.trim_start().strip_prefix('(')?;
            let labels_end = labels.find(')')?;
            let labels_text = &labels[..labels_end];
            return Some((
                format!("{modifier} ({labels_text})"),
                &labels[labels_end + 1..],
            ));
        }
    }
    None
}
