use super::*;

#[test]
pub(crate) fn iter_int_cells_visits_every_window() {
    let range_array = int_range_array();

    let collected = range_array
        .iter_timestamp_slices()
        .unwrap()
        .map(<[i64]>::to_vec)
        .collect::<Vec<_>>();
    assert2::assert!(collected == vec![vec![0, 15], vec![30, 45]]);
}
