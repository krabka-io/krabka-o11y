use super::*;

#[derive(bon::Builder)]
struct PairedWindowExpectation {
    index: usize,
    values: [f64; 3],
    timestamps: [i64; 3],
}

#[test]
pub(crate) fn paired_builder_shares_window_offsets_across_value_and_timestamp() {
    let values = Float64Array::from(vec![10.0, 11.0, 12.0, 13.0, 14.0]);
    let timestamps = Int64Array::from(vec![0_i64, 15, 30, 45, 60]);
    let paired = RangeArray::from_paired_ranges(
        values,
        timestamps,
        [RangeWindow::new(0, 3), RangeWindow::new(2, 3)],
    )
    .unwrap();
    let value_ranges = paired.values;
    let ts_ranges = paired.timestamps;

    assert2::assert!(value_ranges.ranges() == ts_ranges.ranges());
    assert2::assert!(value_ranges.len() == 2);

    for expectation in [
        PairedWindowExpectation::builder()
            .index(0)
            .values([10.0, 11.0, 12.0])
            .timestamps([0_i64, 15, 30])
            .build(),
        PairedWindowExpectation::builder()
            .index(1)
            .values([12.0, 13.0, 14.0])
            .timestamps([30, 45, 60])
            .build(),
    ] {
        assert2::assert!(
            value_ranges.value_slice(expectation.index).unwrap() == expectation.values
        );
        assert2::assert!(
            ts_ranges.timestamp_slice(expectation.index).unwrap() == expectation.timestamps
        );
    }
}
