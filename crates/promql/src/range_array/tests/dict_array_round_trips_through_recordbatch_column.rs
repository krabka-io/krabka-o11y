use super::*;

#[test]
pub(crate) fn dict_array_round_trips_through_recordbatch_column() {
    let values = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])) as ArrayRef;
    let range_array = RangeArray::from_ranges(values, [(0_u32, 2_u32), (1, 3)]).unwrap();
    let dict = range_array.clone().into_dict_array().unwrap();
    let back = RangeArray::try_from_dict_array(&dict).unwrap();
    assert2::assert!(back.len() == range_array.len());

    for (index, want) in [(0, vec![1.0, 2.0]), (1, vec![2.0, 3.0, 4.0])] {
        let window = back.get(index).unwrap();
        let window = window.as_any().downcast_ref::<Float64Array>().unwrap();
        assert2::assert!(
            (0..window.len())
                .map(|i| window.value(i))
                .collect::<Vec<_>>()
                == want
        );
    }
}
