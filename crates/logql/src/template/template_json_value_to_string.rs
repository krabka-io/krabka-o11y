use super::format_template_float;

pub(crate) fn template_json_value_to_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "<no value>".to_string(),
        serde_json::Value::Bool(value) => value.to_string(),
        serde_json::Value::Number(value) => {
            if value.is_f64() {
                value
                    .as_f64()
                    .map_or_else(|| value.to_string(), format_template_float)
            } else {
                value.to_string()
            }
        }
        serde_json::Value::String(value) => value.clone(),
        serde_json::Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(template_json_value_to_string)
                .collect::<Vec<_>>()
                .join(" ")
        ),
        serde_json::Value::Object(values) => format!(
            "map[{}]",
            values
                .iter()
                .map(|(key, value)| format!("{key}:{}", template_json_value_to_string(value)))
                .collect::<Vec<_>>()
                .join(" ")
        ),
    }
}
