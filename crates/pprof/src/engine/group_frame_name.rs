pub(crate) fn group_frame_name(labels: &[crate::ProfileLabel]) -> String {
    if labels.len() == 1 {
        labels[0].value.clone()
    } else {
        labels
            .iter()
            .map(|label| format!("{}={}", label.name, label.value))
            .collect::<Vec<_>>()
            .join(", ")
    }
}
