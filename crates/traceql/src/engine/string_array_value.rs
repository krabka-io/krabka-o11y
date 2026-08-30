use super::{Array, DictionaryArray, Int32Type, LargeStringArray, StringArray, StringViewArray};

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
