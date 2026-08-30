use super::Labels;

pub(crate) fn expand_prometheus_alert_template(
    template: &str,
    labels: &Labels,
    value: &str,
) -> String {
    let mut expanded = String::with_capacity(template.len());
    let mut remaining = template;
    while let Some(start) = remaining.find("{{") {
        expanded.push_str(&remaining[..start]);
        let action_start = start + "{{".len();
        let action = &remaining[action_start..];
        let Some(end) = action.find("}}") else {
            expanded.push_str(&remaining[start..]);
            return expanded;
        };
        let expression = action[..end].trim();
        if expression == "$value" {
            expanded.push_str(value);
        } else if let Some(name) = expression.strip_prefix("$labels.") {
            if let Some(label_value) = labels.get(name) {
                expanded.push_str(label_value);
            } else {
                expanded.push_str("{{");
                expanded.push_str(&action[..end]);
                expanded.push_str("}}");
            }
        } else {
            expanded.push_str("{{");
            expanded.push_str(&action[..end]);
            expanded.push_str("}}");
        }
        remaining = &action[end + "}}".len()..];
    }
    expanded.push_str(remaining);
    expanded
}
