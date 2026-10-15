use super::{
    ATTR_PREFIX, Array, AsArray, AttrValue, Attribute, BLOCK_ATTR_KEYS, BLOCK_ATTR_VALUE,
    BLOCK_ATTR_VALUE_BOOL, BLOCK_ATTR_VALUE_DOUBLE, BLOCK_ATTR_VALUE_INT, DataType,
    DisplayAttribute, ListArray, RecordBatch, Result, StringArray, TraceqlError, block_row_attrs,
    bool_attr_values, f64_attr_values, i64_attr_values, optional_list_column, string_array_value,
    string_attr_values,
};

// Attr Value Display
pub(crate) fn attr_value_display(value: &AttrValue) -> String {
    match value {
        AttrValue::Unsupported(_) => String::new(),
        AttrValue::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(attr_value_display)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        AttrValue::Str(value) => value.clone(),
        AttrValue::Int(value) => value.to_string(),
        AttrValue::Float(value) => value.to_string(),
        AttrValue::Bool(value) => value.to_string(),
    }
}

// Block Attr Values For Key
pub(crate) fn block_attr_values_for_key(
    str_values: Option<&ListArray>,
    int_values: Option<&ListArray>,
    double_values: Option<&ListArray>,
    bool_values: Option<&ListArray>,
    row: usize,
    attr_idx: usize,
) -> Result<Vec<AttrValue>> {
    let values = string_attr_values(str_values, row, attr_idx, BLOCK_ATTR_VALUE)?;
    if !values.is_empty() {
        return Ok(values.into_iter().map(AttrValue::Str).collect());
    }
    let values = i64_attr_values(int_values, row, attr_idx, BLOCK_ATTR_VALUE_INT)?;
    if !values.is_empty() {
        return Ok(values.into_iter().map(AttrValue::Int).collect());
    }
    let values = f64_attr_values(double_values, row, attr_idx, BLOCK_ATTR_VALUE_DOUBLE)?;
    if !values.is_empty() {
        return Ok(values.into_iter().map(AttrValue::Float).collect());
    }
    Ok(
        bool_attr_values(bool_values, row, attr_idx, BLOCK_ATTR_VALUE_BOOL)?
            .into_iter()
            .map(AttrValue::Bool)
            .collect(),
    )
}

// Block Row Attrs Where
/// Decodes a block row's attributes, skipping each key `keep_key` rejects
/// before its values are read.
///
/// A key flagged as an array yields one [`AttrValue::Array`]; any other key
/// yields one pair per value. A row without the `attr_keys` column, or with a
/// null entry in it, has no attributes.
///
/// # Errors
///
/// Returns [`TraceqlError::Exec`] when an attribute column has the wrong
/// Arrow type.
pub fn block_row_attrs_where(
    batch: &RecordBatch,
    row: usize,
    keep_key: impl Fn(&str) -> bool,
) -> Result<Vec<Attribute>> {
    let Some(keys) = optional_list_column(batch, BLOCK_ATTR_KEYS)? else {
        return Ok(Vec::new());
    };
    if keys.is_null(row) {
        return Ok(Vec::new());
    }
    let key_values = keys.value(row);
    let key_values = crate::string_array_from_arrow(key_values.as_ref(), || {
        TraceqlError::Exec("attr_keys row is not Utf8".into())
    })?;
    let str_values = optional_list_column(batch, BLOCK_ATTR_VALUE)?;
    let int_values = optional_list_column(batch, BLOCK_ATTR_VALUE_INT)?;
    let double_values = optional_list_column(batch, BLOCK_ATTR_VALUE_DOUBLE)?;
    let bool_values = optional_list_column(batch, BLOCK_ATTR_VALUE_BOOL)?;

    let unsupported = optional_list_column(batch, "attr_value_unsupported")?;
    let opaque = unsupported
        .filter(|array| !array.is_null(row))
        .map(|array| array.value(row));
    let opaque = opaque
        .as_ref()
        .and_then(|array| array.as_any().downcast_ref::<StringArray>());
    let array_flags = optional_list_column(batch, "attr_is_array")?;
    let flags = array_flags
        .filter(|array| !array.is_null(row))
        .map(|array| array.value(row));
    let flags = flags
        .as_ref()
        .and_then(|array| array.as_any().downcast_ref::<arrow::array::BooleanArray>());
    let mut out = Vec::new();
    for attr_idx in 0..key_values.len() {
        if key_values.is_null(attr_idx) {
            continue;
        }
        let key = key_values.value(attr_idx);
        if !keep_key(key) {
            continue;
        }
        if let Some(opaque) =
            opaque.filter(|values| attr_idx < values.len() && !values.is_null(attr_idx))
        {
            out.push(Attribute::new(
                key.to_string(),
                AttrValue::Unsupported(opaque.value(attr_idx).to_string()),
            ));
            continue;
        }
        let values = block_attr_values_for_key(
            str_values,
            int_values,
            double_values,
            bool_values,
            row,
            attr_idx,
        )?;
        if flags.is_some_and(|flags| {
            attr_idx < flags.len() && !flags.is_null(attr_idx) && flags.value(attr_idx)
        }) {
            out.push(Attribute::new(key.to_string(), AttrValue::Array(values)));
        } else {
            out.extend(values.into_iter().map(|value| Attribute::new(key, value)));
        }
    }
    Ok(out)
}

// Row Attrs
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

pub(crate) fn push_scoped_attr(
    attrs: &mut Vec<DisplayAttribute>,
    scope: &str,
    key: &str,
    value: &AttrValue,
) {
    attrs.push(DisplayAttribute::new(
        format!("{scope}.{key}"),
        attr_value_display(value),
    ));
}
