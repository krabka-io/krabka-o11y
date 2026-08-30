use super::*;

pub(crate) fn selected_json_value_to_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            serde_json::to_string(value).unwrap_or_default()
        }
        _ => field_value_to_string(value),
    }
}
