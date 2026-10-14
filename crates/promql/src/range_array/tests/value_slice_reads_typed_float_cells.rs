use super::*;

struct FloatSliceExpectation<'a> {
    index: usize,
    expected: Option<&'a [f64]>,
}

#[test]
pub(crate) fn value_slice_reads_typed_float_cells() {
    let range_array = float_range_array();

    for expectation in [
        FloatSliceExpectation {
            index: 0,
            expected: Some(&[1.0, 2.0][..]),
        },
        FloatSliceExpectation {
            index: 1,
            expected: Some(&[2.0, 3.0, 4.0][..]),
        },
        FloatSliceExpectation {
            index: 2,
            expected: None,
        },
    ] {
        assert2::assert!(range_array.value_slice(expectation.index) == expectation.expected);
    }
    // A timestamp accessor on a float backing yields None (wrong type).
    assert2::assert!(range_array.timestamp_slice(0).is_none());
}
