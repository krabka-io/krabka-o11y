use super::*;

pub(crate) fn assert_single_float_sample(
    result: &QueryResult,
    job: &str,
    expected: f64,
    context: &str,
) {
    let sample = lone_unnamed_sample(result, context);
    assert2::assert!(sample.labels.get("job") == Some(job));
    assert2::assert!(approx_eq(float_value(&sample.value), expected));
}

/// Checks that `result` is a vector of one sample without a metric name, and
/// returns that sample.
pub(crate) fn lone_unnamed_sample<'a>(
    result: &'a QueryResult,
    context: &str,
) -> &'a crate::InstantSample {
    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector for {context}");
    };
    assert2::assert!(samples.len() == 1);
    assert2::assert!(samples[0].labels.get("__name__") == None);
    &samples[0]
}
