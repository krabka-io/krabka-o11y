use super::*;

#[test]
pub(crate) fn timestamp_slice_reads_typed_int_cells() {
    let timestamps = Arc::new(Int64Array::from(vec![0_i64, 15, 30, 45])) as ArrayRef;
    let range_array = RangeArray::from_ranges(timestamps, [(0_u32, 2_u32), (2, 2)]).unwrap();

    for (index, want) in [
        (0, Some(&[0_i64, 15][..])),
        (1, Some(&[30, 45][..])),
        (2, None),
    ] {
        assert2::assert!(range_array.timestamp_slice(index) == want);
    }
}
