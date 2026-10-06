use super::Labels;

pub(crate) fn info_identifying_key(labels: &Labels) -> Option<String> {
    if labels.get_value("job").is_none() && labels.get_value("instance").is_none() {
        return None;
    }
    let empty = crate::PromqlString::default();
    Some(
        Labels::from_pairs([
            ("job", labels.get_value("job").unwrap_or(&empty)),
            ("instance", labels.get_value("instance").unwrap_or(&empty)),
        ])
        .order_key(),
    )
}
