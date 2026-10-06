use super::{
    Array, AsArray, DataType, RecordBatch, Result, TraceqlError, bytes_to_hex, string_array_value,
};

pub(crate) fn metric_label_value(batch: &RecordBatch, column: &str, row: usize) -> Result<String> {
    let array = batch
        .column_by_name(column)
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {column}")))?;
    if array.is_null(row) {
        return Ok("nil".to_owned());
    }
    match array.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            string_array_value(array.as_ref(), row)
                .ok_or_else(|| TraceqlError::Exec("unsupported string column type".into()))
        }
        DataType::Dictionary(_, value_type)
            if matches!(
                value_type.as_ref(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) =>
        {
            string_array_value(array.as_ref(), row)
                .ok_or_else(|| TraceqlError::Exec("unsupported string column type".into()))
        }
        DataType::Int64 => Ok(array
            .as_primitive::<arrow::datatypes::Int64Type>()
            .value(row)
            .to_string()),
        DataType::Float64 => Ok(array
            .as_primitive::<arrow::datatypes::Float64Type>()
            .value(row)
            .to_string()),
        DataType::Boolean => Ok(array.as_boolean().value(row).to_string()),
        DataType::Int32 => Ok(array
            .as_primitive::<arrow::datatypes::Int32Type>()
            .value(row)
            .to_string()),
        DataType::FixedSizeBinary(_) => Ok(bytes_to_hex(array.as_fixed_size_binary().value(row))),
        DataType::Struct(_) => {
            let key = column.strip_prefix(super::ATTR_PREFIX).ok_or_else(|| {
                TraceqlError::Exec("typed attributes require an attribute column".into())
            })?;
            let value = super::row_attrs(batch, row)?
                .into_iter()
                .find(|(name, _)| name == key)
                .map(|(_, value)| match value {
                    super::AttrValue::Unsupported(_) => "nil".into(),
                    super::AttrValue::Array(value) => format!("{value:?}"),
                    super::AttrValue::Str(value) => value,
                    super::AttrValue::Int(value) => value.to_string(),
                    super::AttrValue::Float(value) => value.to_string(),
                    super::AttrValue::Bool(value) => value.to_string(),
                });
            Ok(value.unwrap_or_else(|| "nil".into()))
        }
        other => Err(TraceqlError::Exec(format!(
            "unsupported metrics label column type {other:?}"
        ))),
    }
}
