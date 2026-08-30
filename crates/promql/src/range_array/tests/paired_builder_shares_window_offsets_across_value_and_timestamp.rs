use super::*;

#[test]
pub(crate) fn paired_builder_shares_window_offsets_across_value_and_timestamp() {
    let values = Float64Array::from(vec![10.0, 11.0, 12.0, 13.0, 14.0]);
    let timestamps = Int64Array::from(vec![0_i64, 15, 30, 45, 60]);
    let (value_ranges, ts_ranges) =
        RangeArray::from_paired_ranges(values, timestamps, [(0_u32, 3_u32), (2, 3)]).unwrap();

    assert2::assert!(value_ranges.ranges() == ts_ranges.ranges());
    assert2::assert!(value_ranges.len() == 2);

    for (index, want_values, want_timestamps) in [
        (0, [10.0, 11.0, 12.0], [0_i64, 15, 30]),
        (1, [12.0, 13.0, 14.0], [30, 45, 60]),
    ] {
        assert2::assert!(value_ranges.value_slice(index).unwrap() == want_values);
        assert2::assert!(ts_ranges.timestamp_slice(index).unwrap() == want_timestamps);
    }
}
