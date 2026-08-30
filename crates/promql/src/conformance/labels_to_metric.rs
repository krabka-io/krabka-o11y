use super::*;

pub(crate) fn labels_to_metric(labels: &Labels) -> String {
    let name = labels.get("__name__").unwrap_or_default();
    let pairs = labels
        .iter()
        .filter(|(label, _)| *label != "__name__")
        .map(|(label, value)| format!(r#"{label}="{}""#, escape_label_value(value)))
        .collect::<Vec<_>>();
    if pairs.is_empty() {
        return name.to_string();
    }
    format!("{name}{{{}}}", pairs.join(","))
}
