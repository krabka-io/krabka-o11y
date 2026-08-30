use super::*;

impl InstantSampleExt for InstantSample {
    fn value_f64(&self) -> f64 {
        match &self.value {
            SampleValue::Float(value) => *value,
            SampleValue::Histogram(_) => panic!("expected float sample"),
        }
    }
}
