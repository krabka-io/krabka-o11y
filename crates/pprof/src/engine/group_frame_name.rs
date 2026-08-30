pub(crate) fn group_frame_name(labels: &[(String, String)]) -> String {
    if labels.len() == 1 {
        labels[0].1.clone()
    } else {
        labels
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join(", ")
    }
}
