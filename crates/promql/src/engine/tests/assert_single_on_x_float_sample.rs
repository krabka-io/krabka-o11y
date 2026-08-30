use super::*;

pub(crate) fn assert_single_on_x_float_sample(result: &QueryResult, expected: f64, context: &str) {
    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector for {context}");
    };
    assert2::assert!(samples.len() == 1);
    assert2::assert!(samples[0].labels.get("__name__") == None);
    assert2::assert!(samples[0].labels.get("job") == None);
    assert2::assert!(samples[0].labels.get("x") == Some("1"));
    assert2::assert!(approx_eq(float_value(&samples[0].value), expected));
}
