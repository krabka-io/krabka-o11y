use super::*;

#[test]
pub(crate) fn iter_float_cells_visits_every_window() {
    let range_array = float_range_array();

    let collected = range_array
        .iter_value_slices()
        .unwrap()
        .map(<[f64]>::to_vec)
        .collect::<Vec<_>>();
    assert2::assert!(collected == vec![vec![1.0, 2.0], vec![2.0, 3.0, 4.0]]);
}
