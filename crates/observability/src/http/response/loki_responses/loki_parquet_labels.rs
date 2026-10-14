use super::{HttpQueryError, Value};

pub(crate) fn loki_parquet_labels(
    labels: Option<&Value>,
    field: &'static str,
) -> Result<Vec<krabka_blockstore::LabelPair>, HttpQueryError> {
    let labels = labels
        .and_then(Value::as_object)
        .ok_or(HttpQueryError::LokiParquet(field))?;
    labels
        .iter()
        .map(|entry| {
            let key = entry.0;
            let value = entry.1;
            value.as_str().map_or_else(
                || Err(HttpQueryError::LokiParquet("label value is not a string")),
                |value| Ok(krabka_blockstore::LabelPair::new(key.clone(), value)),
            )
        })
        .collect()
}
