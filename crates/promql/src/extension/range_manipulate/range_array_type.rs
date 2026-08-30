use super::{Arc, DataType, Field};

/// Returns the Arrow `DataType` of a [`RangeArray`] column with `value_type` samples.
///
/// A `RangeArray` uses a dictionary of per-cell lists,
/// `Dictionary<Int64, List<value_type>>`. This matches
/// [`RangeArray::into_dict_array`].
#[must_use]
pub(crate) fn range_array_type(value_type: DataType, nullable: bool) -> DataType {
    let item = Field::new("item", value_type, nullable);
    DataType::Dictionary(
        Box::new(DataType::Int64),
        Box::new(DataType::List(Arc::new(item))),
    )
}
