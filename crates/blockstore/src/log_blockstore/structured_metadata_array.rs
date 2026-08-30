use super::*;

pub(crate) fn structured_metadata_array(rows: &[LogRow]) -> Result<MapArray, BlockStoreError> {
    let mut builder = MapBuilder::new(
        Some(datafusion::arrow::array::MapFieldNames {
            entry: "entries".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        }),
        StringBuilder::new(),
        StringBuilder::new(),
    )
    .with_values_field(Arc::new(Field::new("value", DataType::Utf8, false)));

    for row in rows {
        for (key, value) in &row.structured_metadata {
            builder.keys().append_value(key);
            builder.values().append_value(value);
        }
        builder.append(true)?;
    }

    Ok(builder.finish())
}
