use super::*;

/// A single sample value: a float or a native histogram.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum SampleValue {
    Float(f64),
    Histogram(NativeHistogram),
}
