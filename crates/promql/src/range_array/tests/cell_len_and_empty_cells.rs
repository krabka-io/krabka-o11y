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
