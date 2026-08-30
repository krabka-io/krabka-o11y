use super::*;

#[test]
pub(crate) fn iter_int_cells_visits_every_window() {
    let timestamps = Arc::new(Int64Array::from(vec![0_i64, 15, 30, 45])) as ArrayRef;
    let range_array = RangeArray::from_ranges(timestamps, [(0_u32, 2_u32), (2, 2)]).unwrap();

    let collected = range_array
        .iter_timestamp_slices()
        .unwrap()
        .map(<[i64]>::to_vec)
        .collect::<Vec<_>>();
    assert2::assert!(collected == vec![vec![0, 15], vec![30, 45]]);
}
