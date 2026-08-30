use super::*;

pub(crate) fn field_type_from_json(value: &Value) -> DetectedFieldType {
    match value {
        Value::Bool(_) => DetectedFieldType::Boolean,
        Value::Number(number) => {
            if number.is_i64() || number.is_u64() {
                DetectedFieldType::Int
            } else {
                DetectedFieldType::Float
            }
        }
        Value::String(value) => field_type_from_str(value),
        Value::Null | Value::Array(_) | Value::Object(_) => DetectedFieldType::String,
    }
}
