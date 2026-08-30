use super::{Labels, format_sample_value};

pub(crate) fn expand_alert_action(action: &str, value: f64, labels: &Labels) -> Option<String> {
    if action == "$value" {
        return Some(format_sample_value(value));
    }
    if let Some(label_ref) = action.strip_prefix("$labels.") {
        let name = label_ref.trim();
        let name = name
            .strip_prefix('"')
            .and_then(|inner| inner.strip_suffix('"'))
            .unwrap_or(name);
        let resolved = labels
            .iter()
            .find(|(label, _)| label.as_str() == name)
            .map(|(_, label_value)| label_value.clone())
            .unwrap_or_default();
        return Some(resolved);
    }
    None
}
