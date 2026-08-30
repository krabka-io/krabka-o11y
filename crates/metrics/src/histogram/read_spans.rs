use super::{Array, BucketSpan, HistogramCodecError, Int32Array, ListArray, StructArray, UInt32Array, require_non_null, schema_mismatch};

pub(crate) fn read_spans(
    list: &ListArray,
    row: usize,
    column: &str,
) -> Result<Vec<BucketSpan>, HistogramCodecError> {
    require_non_null(list, row, column)?;
    let value = list.value(row);
    let struct_array = value
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| schema_mismatch(column))?;
    if struct_array.num_columns() < 2 {
        return Err(schema_mismatch(column));
    }
    let offsets = struct_array
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| schema_mismatch(column))?;
    let lengths = struct_array
        .column(1)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| schema_mismatch(column))?;

    (0..struct_array.len())
        .map(|index| {
            require_non_null(struct_array, index, column)?;
            require_non_null(offsets, index, column)?;
            require_non_null(lengths, index, column)?;
            Ok(BucketSpan {
                offset: offsets.value(index),
                length: lengths.value(index),
            })
        })
        .collect()
}
