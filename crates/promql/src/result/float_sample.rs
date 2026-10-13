/// A floating point sample before it is paired with series labels.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FloatSample {
    pub timestamp_ms: i64,
    pub value: f64,
}
