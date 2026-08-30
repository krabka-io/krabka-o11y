use super::*;

#[test]
pub(crate) fn cell_len_and_empty_cells() {
    let values = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef;
    let range_array = RangeArray::from_ranges(values, [(0_u32, 2_u32), (2, 0), (1, 1)]).unwrap();

    for (index, want) in [(0, Some(2)), (1, Some(0)), (2, Some(1)), (3, None)] {
        assert2::assert!(range_array.cell_len(index) == want);
    }

    // An empty cell yields an empty slice, not None.
    assert2::assert!(range_array.value_slice(1).unwrap().is_empty());
}
