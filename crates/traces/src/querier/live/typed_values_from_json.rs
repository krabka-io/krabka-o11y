use super::*;

pub(crate) fn typed_values_from_json(json: &serde_json::Value) -> Result<Vec<TypedValue>> {
    let values = json
        .get("tagValues")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            TraceqlError::Plan("remote live-store tag values response missing tagValues".into())
        })?;
    Ok(values
        .iter()
        .filter_map(|value| {
            Some(TypedValue {
                type_: value.get("type")?.as_str()?.to_string(),
                value: value.get("value")?.as_str()?.to_string(),
            })
        })
        .collect())
}
