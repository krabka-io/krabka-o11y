use super::*;

#[test]
pub(crate) fn out_of_bounds_window_is_rejected() {
    let values = Arc::new(Float64Array::from(vec![1.0, 2.0])) as ArrayRef;
    assert2::assert!(RangeArray::from_ranges(values, [(1_u32, 5_u32)]).is_err());
}
