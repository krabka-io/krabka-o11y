use super::*;

pub(crate) fn otlp_severity_number_to_string(value: &Value) -> Result<String, DistributorError> {
    match value {
        Value::Number(number) => Ok(number.to_string()),
        Value::String(string) => Ok(string.clone()),
        _ => Err(DistributorError::InvalidOtlpPayload),
    }
}
