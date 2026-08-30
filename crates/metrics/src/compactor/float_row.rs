
/// One sorted float sample row ready for block encoding.
#[derive(Clone, Debug, PartialEq)]
pub struct FloatRow {
    pub fingerprint: u64,
    pub timestamp_ms: i64,
    pub value: f64,
}
