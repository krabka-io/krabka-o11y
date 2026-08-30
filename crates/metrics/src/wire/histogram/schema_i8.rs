use super::WireError;

pub(crate) fn schema_i8(schema: i32) -> Result<i8, WireError> {
    if schema == -53 || (-4..=8).contains(&schema) {
        i8::try_from(schema)
            .map_err(|_| WireError::Invalid(format!("histogram schema {schema} out of range")))
    } else {
        Err(WireError::Invalid(format!(
            "histogram schema {schema} is not supported"
        )))
    }
}
