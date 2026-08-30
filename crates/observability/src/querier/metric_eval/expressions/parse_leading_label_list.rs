pub(crate) fn parse_leading_label_list(query: &str) -> Option<(Vec<String>, &str)> {
    let inner = query.strip_prefix('(')?;
    let labels_end = inner.find(')')?;
    let labels_text = &inner[..labels_end];
    let labels = if labels_text.trim().is_empty() {
        Vec::new()
    } else {
        labels_text
            .split(',')
            .map(str::trim)
            .filter(|label| !label.is_empty())
            .map(str::to_string)
            .collect()
    };
    Some((labels, &inner[labels_end + 1..]))
}
