use super::*;

struct TimestampSliceExpectation<'a> {
    index: usize,
    expected: Option<&'a [i64]>,
}

#[test]
pub(crate) fn timestamp_slice_reads_typed_int_cells() {
    let timestamps = Arc::new(Int64Array::from(vec![0_i64, 15, 30, 45])) as ArrayRef;
    let range_array =
        RangeArray::from_ranges(timestamps, [RangeWindow::new(0, 2), RangeWindow::new(2, 2)])
            .unwrap();

    for expectation in [
        TimestampSliceExpectation {
            index: 0,
            expected: Some(&[0_i64, 15][..]),
        },
        TimestampSliceExpectation {
            index: 1,
            expected: Some(&[30, 45][..]),
        },
        TimestampSliceExpectation {
            index: 2,
            expected: None,
        },
    ] {
        assert2::assert!(range_array.timestamp_slice(expectation.index) == expectation.expected);
    }
}
