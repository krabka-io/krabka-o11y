use super::*;

#[test]
pub(crate) fn dict_array_round_trips_through_recordbatch_column() {
    let values = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])) as ArrayRef;
    let range_array = RangeArray::from_ranges(values, [(0_u32, 2_u32), (1, 3)]).unwrap();
    let dict = range_array.clone().into_dict_array().unwrap();
    let back = RangeArray::try_from_dict_array(&dict).unwrap();
    assert2::assert!(back.len() == range_array.len());

    for (index, want) in [(0, vec![1.0, 2.0]), (1, vec![2.0, 3.0, 4.0])] {
        assert2::assert!(window_floats(&back, index) == want);
    }
}
