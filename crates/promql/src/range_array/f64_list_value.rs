use super::{Array, Float64Array, ListArray, list_offsets};

pub(crate) fn f64_list_value(list: &ListArray, row: usize) -> Option<&[f64]> {
    let (start, end) = list_offsets(list, row)?;
    let values = list.values().as_any().downcast_ref::<Float64Array>()?;
    Some(&values.values()[start..end])
}
