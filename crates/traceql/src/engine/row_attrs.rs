use super::{
    ATTR_PREFIX, Array, AsArray, AttrValue, Attribute, DataType, RecordBatch, Result, TraceqlError,
    block_row_attrs, string_array_value,
};

pub(crate) fn row_attrs(batch: &RecordBatch, row: usize) -> Result<Vec<Attribute>> {
    let schema = batch.schema();
    let mut attrs = Vec::new();
    for indexed_field in schema.fields().iter().enumerate() {
        let idx = indexed_field.0;
        let field = indexed_field.1;
        let Some(name) = field.name().strip_prefix(ATTR_PREFIX) else {
            continue;
        };
        let array = batch.column(idx);
        if array.is_null(row) {
            continue;
        }
        let value = match field.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                AttrValue::Str(string_array_value(array.as_ref(), row).ok_or_else(|| {
                    TraceqlError::Exec(format!("unsupported string attribute column {name}"))
                })?)
            }
            DataType::Dictionary(_, value_type)
                if matches!(
                    value_type.as_ref(),
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                ) =>
            {
                AttrValue::Str(string_array_value(array.as_ref(), row).ok_or_else(|| {
                    TraceqlError::Exec(format!("unsupported dictionary attribute column {name}"))
                })?)
            }
            DataType::Int64 => AttrValue::Int(
                array
                    .as_primitive::<arrow::datatypes::Int64Type>()
                    .value(row),
            ),
            DataType::Float64 => AttrValue::Float(
                array
                    .as_primitive::<arrow::datatypes::Float64Type>()
                    .value(row),
            ),
            DataType::Boolean => AttrValue::Bool(array.as_boolean().value(row)),
            DataType::Struct(_) => {
                let typed = array.as_struct();
                let mut typed_values = Vec::new();
                let is_array = typed
                    .column_by_name("is_array")
                    .is_some_and(|flags| !flags.is_null(row) && flags.as_boolean().value(row));
                for named_column in typed.column_names().iter().zip(typed.columns()) {
                    let key = named_column.0;
                    let value = named_column.1;
                    if *key == "is_array" {
                        continue;
                    }
                    if !matches!(value.data_type(), DataType::List(_)) {
                        return Err(TraceqlError::Exec("invalid typed attribute list".into()));
                    }
                    let values = value.as_list::<i32>().value(row);
                    for index in 0..values.len() {
                        if values.is_null(index) {
                            continue;
                        }
                        let value = match *key {
                            "string" => AttrValue::Str(
                                string_array_value(values.as_ref(), index).ok_or_else(|| {
                                    TraceqlError::Exec("invalid typed string attribute".into())
                                })?,
                            ),
                            "int" => AttrValue::Int(
                                values
                                    .as_primitive::<arrow::datatypes::Int64Type>()
                                    .value(index),
                            ),
                            "float" => AttrValue::Float(
                                values
                                    .as_primitive::<arrow::datatypes::Float64Type>()
                                    .value(index),
                            ),
                            "bool" => AttrValue::Bool(values.as_boolean().value(index)),
                            _ => {
                                return Err(TraceqlError::Exec(
                                    "invalid typed attribute field".into(),
                                ));
                            }
                        };
                        typed_values.push(value);
                    }
                }
                if is_array {
                    attrs.push(Attribute::new(
                        name.to_string(),
                        AttrValue::Array(typed_values),
                    ));
                } else {
                    attrs.extend(
                        typed_values
                            .into_iter()
                            .map(|value| Attribute::new(name.to_string(), value)),
                    );
                }
                continue;
            }
            other => {
                return Err(TraceqlError::Exec(format!(
                    "unsupported attribute column type {other:?}"
                )));
            }
        };
        attrs.push(Attribute::new(name.to_string(), value));
    }
    attrs.extend(block_row_attrs(batch, row)?);
    attrs.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(attrs)
}
