use super::*;

#[test]
pub(crate) fn windows_slice_the_backing_array() {
    let mut builder = Float64Builder::new();
    for value in [10.0, 11.0, 12.0, 13.0, 14.0] {
        builder.append_value(value);
    }
    let values = Arc::new(builder.finish()) as ArrayRef;

    let range_array = RangeArray::from_ranges(values, [(0_u32, 3_u32), (2, 3)]).unwrap();
    assert2::assert!(range_array.len() == 2);

    for (index, want) in [(0, vec![10.0, 11.0, 12.0]), (1, vec![12.0, 13.0, 14.0])] {
        assert2::assert!(window_floats(&range_array, index) == want);
    }
}
