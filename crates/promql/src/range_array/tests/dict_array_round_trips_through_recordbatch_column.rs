use super::*;

#[derive(bon::Builder)]
struct RoundTripWindowExpectation {
    index: usize,
    values: Vec<f64>,
}

#[test]
pub(crate) fn dict_array_round_trips_through_recordbatch_column() {
    let values = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])) as ArrayRef;
    let range_array =
        RangeArray::from_ranges(values, [RangeWindow::new(0, 2), RangeWindow::new(1, 3)]).unwrap();
    let dict = range_array.clone().into_dict_array().unwrap();
    let back = RangeArray::try_from_dict_array(&dict).unwrap();
    assert2::assert!(back.len() == range_array.len());

    for expectation in [
        RoundTripWindowExpectation::builder()
            .index(0)
            .values(vec![1.0, 2.0])
            .build(),
        RoundTripWindowExpectation::builder()
            .index(1)
            .values(vec![2.0, 3.0, 4.0])
            .build(),
    ] {
        assert2::assert!(window_floats(&back, expectation.index) == expectation.values);
    }
}
