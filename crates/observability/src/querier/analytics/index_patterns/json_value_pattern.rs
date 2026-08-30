use super::{Value, templatize_text};

/// Replaces variable JSON leaf values with the `<_>` placeholder. It keeps the
/// object and array structure, and it keeps constant, low-entropy, values.
pub(crate) fn json_value_pattern(value: &Value) -> Value {
    match value {
        // Numbers are always high-cardinality dimensions (offsets, durations,
        // counts), so collapse them; booleans and null are constants worth
        // keeping as discriminators.
        Value::Number(_) => Value::String("<_>".to_string()),
        Value::Null | Value::Bool(_) => value.clone(),
        Value::String(text) => Value::String(templatize_text(text)),
        Value::Array(items) => Value::Array(items.iter().map(json_value_pattern).collect()),
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(key, value)| (key.clone(), json_value_pattern(value)))
                .collect(),
        ),
    }
}
