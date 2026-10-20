use super::{
    Array, BooleanArray, DictionaryArray, Float64Array, Int32Type, Int64Array, LargeStringArray,
    ListArray, Result, StringArray, StringViewArray, TraceqlError,
};

// Bool Attr Values
pub(crate) fn bool_attr_values(
    values: Option<&ListArray>,
    row: usize,
    attr_idx: usize,
    name: &str,
) -> Result<Vec<bool>> {
    let Some(values) = row_attr_values(values, row, attr_idx, name)? else {
        return Ok(Vec::new());
    };
    let values = values
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| TraceqlError::Exec(format!("attribute column `{name}` is not Boolean")))?;
    Ok((0..values.len())
        .filter(|idx| !values.is_null(*idx))
        .map(|idx| values.value(idx))
        .collect())
}

// F64 Attr Values
pub(crate) fn f64_attr_values(
    values: Option<&ListArray>,
    row: usize,
    attr_idx: usize,
    name: &str,
) -> Result<Vec<f64>> {
    let Some(values) = row_attr_values(values, row, attr_idx, name)? else {
        return Ok(Vec::new());
    };
    let values = values
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| TraceqlError::Exec(format!("attribute column `{name}` is not Float64")))?;
    Ok((0..values.len())
        .filter(|idx| !values.is_null(*idx))
        .map(|idx| values.value(idx))
        .collect())
}

// I64 Attr Values
pub(crate) fn i64_attr_values(
    values: Option<&ListArray>,
    row: usize,
    attr_idx: usize,
    name: &str,
) -> Result<Vec<i64>> {
    let Some(values) = row_attr_values(values, row, attr_idx, name)? else {
        return Ok(Vec::new());
    };
    let values = values
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| TraceqlError::Exec(format!("attribute column `{name}` is not Int64")))?;
    Ok((0..values.len())
        .filter(|idx| !values.is_null(*idx))
        .map(|idx| values.value(idx))
        .collect())
}

// Row Attr Values
pub(crate) fn row_attr_values(
    values: Option<&ListArray>,
    row: usize,
    attr_idx: usize,
    name: &str,
) -> Result<Option<arrow::array::ArrayRef>> {
    let Some(values) = values else {
        return Ok(None);
    };
    if values.is_null(row) {
        return Ok(None);
    }
    let row_values = values.value(row);
    let row_values = row_values
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| {
            TraceqlError::Exec(format!("attribute column `{name}` row is not a list"))
        })?;
    if attr_idx >= row_values.len() || row_values.is_null(attr_idx) {
        return Ok(None);
    }
    Ok(Some(row_values.value(attr_idx)))
}

// String Array Value
pub(crate) fn string_array_value(array: &dyn Array, row: usize) -> Option<String> {
    array
        .as_any()
        .downcast_ref::<StringArray>()
        .map(|arr| arr.value(row).to_string())
        .or_else(|| {
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .map(|arr| arr.value(row).to_string())
        })
        .or_else(|| {
            array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .map(|arr| arr.value(row).to_string())
        })
        .or_else(|| {
            array
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .and_then(|arr| {
                    let key = usize::try_from(arr.keys().value(row)).ok()?;
                    string_array_value(arr.values().as_ref(), key)
                })
        })
}

// String Attr Values
pub(crate) fn string_attr_values(
    values: Option<&ListArray>,
    row: usize,
    attr_idx: usize,
    name: &str,
) -> Result<Vec<String>> {
    let Some(values) = row_attr_values(values, row, attr_idx, name)? else {
        return Ok(Vec::new());
    };
    let values = values
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| TraceqlError::Exec(format!("attribute column `{name}` is not Utf8")))?;
    Ok((0..values.len())
        .filter(|idx| !values.is_null(*idx))
        .map(|idx| values.value(idx).to_string())
        .collect())
}
