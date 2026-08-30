use super::{ListArray, HistogramCodecError, require_non_null, Array, Float64Array, schema_mismatch};

pub(crate) fn read_f64_list(
    list: &ListArray,
    row: usize,
    column: &str,
) -> Result<Vec<f64>, HistogramCodecError> {
    require_non_null(list, row, column)?;
    let value = list.value(row);
    let array = value
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| schema_mismatch(column))?;

    (0..array.len())
        .map(|index| {
            require_non_null(array, index, column)?;
            Ok(array.value(index))
        })
        .collect()
}
