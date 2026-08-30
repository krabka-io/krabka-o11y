pub(crate) fn parse_vector_matching_modifier(
    query: &str,
    position: usize,
) -> Option<(String, usize)> {
    for modifier in ["on", "ignoring"] {
        if let Some(rest) = query[position..].strip_prefix(modifier) {
            let labels = rest.strip_prefix('(')?;
            let labels_end = labels.find(')')?;
            let labels = &labels[..labels_end];
            return Some((
                format!("{modifier} ({labels})"),
                position + modifier.len() + 1 + labels_end + 1,
            ));
        }
    }
    None
}
