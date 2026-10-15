use super::{Arc, BlockStoreError, BTreeMap, DataType, Field, Fields, LogRow, MapArray, MapBuilder, StringBuilder};

pub type StructuredMetadata = BTreeMap<String, String>;

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
        for key in row.structured_metadata.keys() {
            builder.keys().append_value(key);
            builder.values().append_value(&row.structured_metadata[key]);
        }
        builder.append(true)?;
    }

    Ok(builder.finish())
}

pub(crate) fn structured_metadata_type() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, false),
            ])),
            false,
        )),
        false,
    )
}

pub(crate) fn structured_metadata_value(
    metadata: &MapArray,
    row: usize,
) -> Result<StructuredMetadata, BlockStoreError> {
    let entries = crate::map_array_string_entries(metadata, row, |column, expected| {
        BlockStoreError::InvalidBlockColumn { column, expected }
    })?;
    let mut result = StructuredMetadata::new();
    for entry in entries {
        result.insert(entry.key, entry.value);
    }
    Ok(result)
}
