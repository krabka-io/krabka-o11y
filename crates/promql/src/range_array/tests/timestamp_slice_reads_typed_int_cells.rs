use super::*;

struct TimestampSliceExpectation<'a> {
    index: usize,
    expected: Option<&'a [i64]>,
}

#[test]
pub(crate) fn timestamp_slice_reads_typed_int_cells() {
    let range_array = int_range_array();

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
