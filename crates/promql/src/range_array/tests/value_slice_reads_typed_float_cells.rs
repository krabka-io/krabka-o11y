use super::*;

#[test]
pub(crate) fn value_slice_reads_typed_float_cells() {
    let values = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])) as ArrayRef;
    let range_array = RangeArray::from_ranges(values, [(0_u32, 2_u32), (1, 3)]).unwrap();

    for (index, want) in [
        (0, Some(&[1.0, 2.0][..])),
        (1, Some(&[2.0, 3.0, 4.0][..])),
        (2, None),
    ] {
        assert2::assert!(range_array.value_slice(index) == want);
    }
    // A timestamp accessor on a float backing yields None (wrong type).
    assert2::assert!(range_array.timestamp_slice(0).is_none());
}
