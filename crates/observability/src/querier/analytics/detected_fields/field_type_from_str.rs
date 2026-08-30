use super::{DetectedFieldType, is_bytes_literal, is_prometheus_duration_literal};

pub(crate) fn field_type_from_str(value: &str) -> DetectedFieldType {
    let normalized = value.to_ascii_lowercase();
    if matches!(normalized.as_str(), "true" | "false") {
        return DetectedFieldType::Boolean;
    }
    if value.parse::<i64>().is_ok() {
        return DetectedFieldType::Int;
    }
    if value.parse::<f64>().is_ok() {
        return DetectedFieldType::Float;
    }
    if is_prometheus_duration_literal(value) {
        return DetectedFieldType::Duration;
    }
    if is_bytes_literal(value) {
        return DetectedFieldType::Bytes;
    }
    DetectedFieldType::String
}
