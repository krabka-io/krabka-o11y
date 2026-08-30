use super::{ArrayRef, DfResult, RangeArray, Array, DictionaryArray, Int64Type, DataFusionError};

/// Decodes a `Dictionary<Int64, List<_>>` range column into a [`RangeArray`].
pub(crate) fn decode_range_column(array: &ArrayRef, arg: &str, udf: &str) -> DfResult<RangeArray> {
    let dict = array
        .as_any()
        .downcast_ref::<DictionaryArray<Int64Type>>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "{udf}: `{arg}` must be a RangeArray dictionary column, got {:?}",
                array.data_type()
            ))
        })?;
    RangeArray::try_from_dict_array(dict)
        .map_err(|error| DataFusionError::Execution(format!("{udf}: decoding `{arg}`: {error}")))
}
