use super::*;

#[test]
pub(crate) fn basic_accessors_report_empty_state_and_exact_ranges() {
    let values = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef;
    let empty = RangeArray::from_ranges(values.clone(), []).unwrap();
    let range_array = RangeArray::from_ranges(values, [(1_u32, 0_u32), (0, 2), (2, 1)]).unwrap();
    assert2::assert!(empty.len() == 0);
    assert2::assert!(empty.is_empty());
    assert2::assert!(empty.ranges() == &[][..]);
    assert2::assert!(range_array.len() == 3);
    assert2::assert!(!range_array.is_empty());
    assert2::assert!(range_array.ranges() == &[(1, 0), (0, 2), (2, 1)][..]);
}
