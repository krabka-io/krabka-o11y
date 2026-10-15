use super::*;

mod basic_accessors_report_empty_state_and_exact_ranges {
    use super::*;

    #[test]
    pub(crate) fn basic_accessors_report_empty_state_and_exact_ranges() {
        let values = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef;
        let empty = RangeArray::from_ranges(values.clone(), []).unwrap();
        let range_array = RangeArray::from_ranges(
            values,
            [
                RangeWindow::new(1, 0),
                RangeWindow::new(0, 2),
                RangeWindow::new(2, 1),
            ],
        )
        .unwrap();
        assert2::assert!(empty.len() == 0);
        assert2::assert!(empty.is_empty());
        assert2::assert!(empty.ranges() == &[][..]);
        assert2::assert!(range_array.len() == 3);
        assert2::assert!(!range_array.is_empty());
        assert2::assert!(
            range_array.ranges()
                == &[
                    RangeWindow::new(1, 0),
                    RangeWindow::new(0, 2),
                    RangeWindow::new(2, 1),
                ][..]
        );
    }
}

mod cell_len_and_empty_cells {
    use super::*;

    struct CellLengthExpectation {
        index: usize,
        length: Option<usize>,
    }

    #[test]
    pub(crate) fn cell_len_and_empty_cells() {
        let values = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef;
        let range_array = RangeArray::from_ranges(
            values,
            [
                RangeWindow::new(0, 2),
                RangeWindow::new(2, 0),
                RangeWindow::new(1, 1),
            ],
        )
        .unwrap();

        for expectation in [
            CellLengthExpectation {
                index: 0,
                length: Some(2),
            },
            CellLengthExpectation {
                index: 1,
                length: Some(0),
            },
            CellLengthExpectation {
                index: 2,
                length: Some(1),
            },
            CellLengthExpectation {
                index: 3,
                length: None,
            },
        ] {
            assert2::assert!(range_array.cell_len(expectation.index) == expectation.length);
        }

        // An empty cell yields an empty slice, not None.
        assert2::assert!(range_array.value_slice(1).unwrap().is_empty());
    }
}

mod timestamp_slice_reads_typed_int_cells {
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
            assert2::assert!(
                range_array.timestamp_slice(expectation.index) == expectation.expected
            );
        }
    }
}

mod typed_accessor_matches_get_over_a_pre_sliced_backing_array {
    use super::*;

    #[test]
    pub(crate) fn typed_accessor_matches_get_over_a_pre_sliced_backing_array() {
        // Build a RangeArray over an already-sliced backing array; the typed
        // zero-copy accessor must agree with the `get()` re-slice path.
        let full = Arc::new(Float64Array::from(vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0])) as ArrayRef;
        let sliced = full.slice(2, 3); // logical [2.0, 3.0, 4.0]
        let range_array =
            RangeArray::from_ranges(sliced, [RangeWindow::new(0, 2), RangeWindow::new(1, 2)])
                .unwrap();

        let via_get = range_array.get(1).unwrap();
        let via_get = via_get.as_any().downcast_ref::<Float64Array>().unwrap();
        let via_get = (0..via_get.len())
            .map(|index| via_get.value(index))
            .collect::<Vec<_>>();
        assert2::assert!(via_get == vec![3.0, 4.0]);
        assert2::assert!(range_array.value_slice(1).unwrap() == via_get.as_slice());
    }
}

mod value_slice_reads_typed_float_cells {
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
}
