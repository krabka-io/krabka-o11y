use super::*;

#[test]
pub(crate) fn iter_float_cells_visits_every_window() {
    let values = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])) as ArrayRef;
    let range_array = RangeArray::from_ranges(values, [(0_u32, 2_u32), (1, 3)]).unwrap();

    let collected = range_array
        .iter_value_slices()
        .unwrap()
        .map(<[f64]>::to_vec)
        .collect::<Vec<_>>();
    assert2::assert!(collected == vec![vec![1.0, 2.0], vec![2.0, 3.0, 4.0]]);
}
