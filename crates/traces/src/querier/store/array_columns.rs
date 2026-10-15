use super::*;

pub(crate) fn fixed_array_value<const N: usize>(
    values: &FixedSizeBinaryArray,
    row: usize,
    name: &str,
) -> Result<[u8; N], TraceqlError> {
    if values.is_null(row) {
        return Ok([0; N]);
    }
    values
        .value(row)
        .try_into()
        .map_err(|_| TraceqlError::Store(format!("bad fixed binary width for `{name}`")))
}

pub(crate) fn insert_i32_value(
    batch: &RecordBatch,
    row: usize,
    values: &mut BTreeSet<super::TypedValue>,
    column: &str,
) -> Result<(), TraceqlError> {
    super::insert_typed_value(values, "int", int32_value(batch, column, row)?.to_string());
    Ok(())
}

pub(crate) fn insert_i64_value(
    batch: &RecordBatch,
    row: usize,
    values: &mut BTreeSet<super::TypedValue>,
    type_: &str,
    column: &str,
) -> Result<(), TraceqlError> {
    values.insert(super::TypedValue::new(
        type_,
        int64_value(batch, column, row)?.to_string(),
    ));
    Ok(())
}

pub(crate) fn insert_string_value(
    batch: &RecordBatch,
    row: usize,
    values: &mut BTreeSet<super::TypedValue>,
    column: &str,
) -> Result<(), TraceqlError> {
    super::insert_typed_value(values, "string", string_value(batch, column, row)?);
    Ok(())
}

pub(crate) fn int32_value(
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<i32, TraceqlError> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| TraceqlError::Store(format!("missing int32 column `{name}`")))?;
    col.as_any()
        .downcast_ref::<Int32Array>()
        .map(|a| a.value(row))
        .ok_or_else(|| TraceqlError::Store("unsupported int32 column type".into()))
}

pub(crate) fn nullable_fixed_value<const N: usize>(
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<Option<[u8; N]>, TraceqlError> {
    let arr = fixed(batch, name)?;
    if arr.is_null(row) {
        Ok(None)
    } else {
        arr.value(row)
            .try_into()
            .map(Some)
            .map_err(|_| TraceqlError::Store(format!("bad fixed binary width for `{name}`")))
    }
}

pub(crate) fn optional_list_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<Option<&'a ListArray>, TraceqlError> {
    batch
        .column_by_name(name)
        .map(|col| {
            col.as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| TraceqlError::Store(format!("nested column `{name}` is not a list")))
        })
        .transpose()
}

/// One `attr.<key>` column a metric projection needs: its name, the attribute
/// key it reads, whether that key is a resource attribute, and its type
/// (`T` is `Option<DataType>` while the type is still being inferred).
pub(crate) struct ProjectedAttrColumn<T> {
    pub(crate) column_name: String,
    pub(crate) lookup_key: String,
    pub(crate) resource: ResourceAttrs,
    pub(crate) data_type: T,
}

pub(crate) fn string_array_value(col: &dyn Array, row: usize) -> Result<String, TraceqlError> {
    col.as_any()
        .downcast_ref::<StringArray>()
        .map(|a| a.value(row).to_string())
        .or_else(|| {
            col.as_any()
                .downcast_ref::<LargeStringArray>()
                .map(|a| a.value(row).to_string())
        })
        .or_else(|| {
            col.as_any()
                .downcast_ref::<StringViewArray>()
                .map(|a| a.value(row).to_string())
        })
        .map(Ok)
        .or_else(|| {
            col.as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .map(|a| {
                    let key = usize::try_from(a.keys().value(row))
                        .map_err(|err| TraceqlError::Store(err.to_string()))?;
                    string_array_value(a.values().as_ref(), key)
                })
        })
        .transpose()?
        .ok_or_else(|| TraceqlError::Store("unsupported string column type".into()))
}

pub(crate) fn string_value(
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<String, TraceqlError> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| TraceqlError::Store(format!("missing string column `{name}`")))?;
    if col.is_null(row) {
        return Ok(String::new());
    }
    string_array_value(col.as_ref(), row)
}

pub(crate) fn struct_fixed_field<'a>(
    values: &'a StructArray,
    field: usize,
    name: &str,
) -> Result<&'a FixedSizeBinaryArray, TraceqlError> {
    values
        .columns()
        .get(field)
        .and_then(|col| col.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .ok_or_else(|| {
            TraceqlError::Store(format!("nested column `{name}` missing fixed binary field"))
        })
}

pub(crate) fn struct_int64_field<'a>(
    values: &'a StructArray,
    field: usize,
    name: &str,
) -> Result<&'a Int64Array, TraceqlError> {
    values
        .columns()
        .get(field)
        .and_then(|col| col.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| TraceqlError::Store(format!("nested column `{name}` missing int64 field")))
}

pub(crate) fn struct_list_field<'a>(
    values: &'a StructArray,
    field: usize,
    name: &str,
) -> Result<&'a ListArray, TraceqlError> {
    values
        .columns()
        .get(field)
        .and_then(|col| col.as_any().downcast_ref::<ListArray>())
        .ok_or_else(|| TraceqlError::Store(format!("nested column `{name}` missing list field")))
}

pub(crate) fn struct_string_field<'a>(
    values: &'a StructArray,
    field: usize,
    name: &str,
) -> Result<&'a dyn Array, TraceqlError> {
    values
        .columns()
        .get(field)
        .map(std::convert::AsRef::as_ref)
        .ok_or_else(|| TraceqlError::Store(format!("nested column `{name}` missing string field")))
}
