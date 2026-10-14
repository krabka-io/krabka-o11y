use super::*;

#[test]
pub(crate) fn out_of_bounds_window_is_rejected() {
    let values = Arc::new(Float64Array::from(vec![1.0, 2.0])) as ArrayRef;
    assert2::assert!(RangeArray::from_ranges(values, [RangeWindow::new(1, 5)]).is_err());
}
