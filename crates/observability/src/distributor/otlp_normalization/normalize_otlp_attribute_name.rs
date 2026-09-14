pub(crate) fn normalize_otlp_attribute_name(name: &str) -> String {
    let mut normalized = name
        .chars()
        .map(|ch| {
            if ch == '_' || ch.is_ascii_alphanumeric() {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if normalized.is_empty() {
        normalized.push('_');
    }
    if normalized
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_digit())
    {
        normalized.insert(0, '_');
    }
    normalized
}

pub(crate) fn matches_otlp_attribute(name: &str, root: &str) -> bool {
    name == root
        || name
            .strip_prefix(root)
            .is_some_and(|rest| rest.starts_with('_'))
}
