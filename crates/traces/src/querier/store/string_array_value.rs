use super::{
    Array, DictionaryArray, Int32Type, LargeStringArray, StringArray, StringViewArray, TraceqlError,
};

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
