use crate::{
    planner::scalar_math::LabeledValue,
    result::{InstantSample, SampleValue},
};

/// Keeps the float samples of an assembled instant vector and drops the
/// histogram ones.
///
/// `simpleFloatFunc`, `clamp`, and the calendar family all process only float
/// samples, so a histogram series is absent from their result rather than an
/// error.
pub fn float_scalar_math_values(samples: Vec<InstantSample>) -> Vec<LabeledValue> {
    samples
        .into_iter()
        .filter_map(|sample| {
            let SampleValue::Float(value) = sample.value else {
                return None;
            };
            Some(LabeledValue {
                labels: sample.labels,
                ts_ms: sample.ts_ms,
                value,
            })
        })
        .collect()
}
