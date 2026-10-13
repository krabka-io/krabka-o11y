use super::{Labels, escape_label_value};

pub(crate) fn labels_to_metric(labels: &Labels) -> String {
    let name = labels.get("__name__").unwrap_or_default();
    let pairs = labels
        .iter()
        .filter(|pair| pair.name != "__name__")
        .map(|pair| format!(r#"{}="{}""#, pair.name, escape_label_value(pair.value)))
        .collect::<Vec<_>>();
    if pairs.is_empty() {
        return name.to_string();
    }
    format!("{name}{{{}}}", pairs.join(","))
}
