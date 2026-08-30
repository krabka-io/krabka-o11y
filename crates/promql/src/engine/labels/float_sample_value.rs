use super::{InstantSample, Result, SampleValue, PromqlError};

pub(crate) fn float_sample_value(sample: &InstantSample) -> Result<f64> {
    match sample.value {
        SampleValue::Float(value) => Ok(value),
        SampleValue::Histogram(_) => Err(PromqlError::Plan(
            "this evaluation path requires a float sample".to_string(),
        )),
    }
}
