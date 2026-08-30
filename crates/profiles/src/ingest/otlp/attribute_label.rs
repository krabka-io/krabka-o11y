use super::{ProfilesError, pb};

pub(crate) fn attribute_label(
    index: i32,
    dict: &pb::otlp_profiles::ProfilesDictionary,
) -> Result<(String, String), ProfilesError> {
    use pb::opentelemetry::proto::common::v1::any_value::Value;

    let attr = usize::try_from(index)
        .ok()
        .and_then(|idx| dict.attribute_table.get(idx))
        .ok_or_else(|| ProfilesError::Invalid("OTLP references missing attribute".into()))?;
    let key_idx = usize::try_from(attr.key_strindex).map_err(|_| {
        ProfilesError::Invalid("OTLP attribute key references missing string".to_string())
    })?;
    let key = dict
        .string_table
        .get(key_idx)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            ProfilesError::Invalid("OTLP attribute key references missing string".to_string())
        })?
        .clone();
    let value = match attr.value.as_ref().and_then(|value| value.value.as_ref()) {
        Some(Value::StringValue(value)) => value.clone(),
        Some(Value::IntValue(value)) => value.to_string(),
        None => String::new(),
    };
    Ok((key, value))
}
