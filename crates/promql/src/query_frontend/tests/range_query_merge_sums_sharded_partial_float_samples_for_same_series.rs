use super::*;

#[test]
pub(crate) fn range_query_merge_sums_sharded_partial_float_samples_for_same_series() {
    let result = merge_range_query_results(vec![
        UnlabeledMatrix::default()
            .at(0, SampleValue::Float(1.0))
            .at(60_000, SampleValue::Float(2.0))
            .matrix(),
        UnlabeledMatrix::default()
            .at(0, SampleValue::Float(10.0))
            .at(60_000, SampleValue::Float(20.0))
            .matrix(),
    ])
    .unwrap();

    assert2::assert!(
        result
            == UnlabeledMatrix::default()
                .at(0, SampleValue::Float(11.0))
                .at(60_000, SampleValue::Float(22.0))
                .matrix()
    );
}
