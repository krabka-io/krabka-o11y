use super::*;

#[derive(bon::Builder)]
struct WindowExpectation {
    index: usize,
    values: Vec<f64>,
}

#[test]
pub(crate) fn windows_slice_the_backing_array() {
    let mut builder = Float64Builder::new();
    for value in [10.0, 11.0, 12.0, 13.0, 14.0] {
        builder.append_value(value);
    }
    let values = Arc::new(builder.finish()) as ArrayRef;

    let range_array =
        RangeArray::from_ranges(values, [RangeWindow::new(0, 3), RangeWindow::new(2, 3)]).unwrap();
    assert2::assert!(range_array.len() == 2);

    for expectation in [
        WindowExpectation::builder()
            .index(0)
            .values(vec![10.0, 11.0, 12.0])
            .build(),
        WindowExpectation::builder()
            .index(1)
            .values(vec![12.0, 13.0, 14.0])
            .build(),
    ] {
        assert2::assert!(window_floats(&range_array, expectation.index) == expectation.values);
    }
}
