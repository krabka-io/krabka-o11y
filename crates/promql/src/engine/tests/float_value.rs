use super::*;

pub(crate) fn float_value(value: &SampleValue) -> f64 {
    match value {
        SampleValue::Float(value) => *value,
        SampleValue::Histogram(_) => panic!("expected float sample"),
    }
}
