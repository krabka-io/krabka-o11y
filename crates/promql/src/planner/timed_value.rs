/// One float sample: an epoch-millisecond timestamp and its value.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TimedValue {
    pub ts_ms: i64,
    pub value: f64,
}
