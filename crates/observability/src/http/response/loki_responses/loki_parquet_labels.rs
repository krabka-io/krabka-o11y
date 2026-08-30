use super::{HttpQueryError, Value};

pub(crate) fn loki_parquet_labels(
    labels: Option<&Value>,
    field: &'static str,
) -> Result<Vec<(String, String)>, HttpQueryError> {
    let labels = labels
        .and_then(Value::as_object)
        .ok_or(HttpQueryError::LokiParquet(field))?;
    labels
        .iter()
        .map(|(key, value)| {
            value.as_str().map_or_else(
                || Err(HttpQueryError::LokiParquet("label value is not a string")),
                |value| Ok((key.clone(), value.to_string())),
            )
        })
        .collect()
}
