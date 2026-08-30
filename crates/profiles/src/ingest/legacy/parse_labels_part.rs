use super::*;

pub(crate) fn parse_labels_part(raw: &[u8]) -> Result<Vec<(String, String)>, ProfilesError> {
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    let json: serde_json::Value = serde_json::from_slice(raw)
        .map_err(|err| ProfilesError::Decode(format!("jfr labels part is not JSON: {err}")))?;
    let object = json.as_object().ok_or_else(|| {
        ProfilesError::Decode("jfr labels part must be a JSON object".to_string())
    })?;
    object
        .iter()
        .map(|(key, value)| {
            let value = match value {
                serde_json::Value::String(value) => value.clone(),
                serde_json::Value::Number(value) => value.to_string(),
                serde_json::Value::Bool(value) => value.to_string(),
                serde_json::Value::Null => String::new(),
                _ => {
                    return Err(ProfilesError::Decode(format!(
                        "jfr label `{key}` must be a scalar"
                    )));
                }
            };
            Ok((key.clone(), value))
        })
        .collect()
}
