/// One decoded float sample from an ingest request.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DecodedSample {
    pub timestamp_ms: i64,
    pub value: f64,
    pub start_timestamp_ms: Option<i64>,
}
impl DecodedSample {
    #[must_use]
    pub fn new(timestamp_ms: i64, value: f64) -> Self {
        Self {
            timestamp_ms,
            value,
            start_timestamp_ms: None,
        }
    }

    #[must_use]
    pub fn with_start_timestamp(
        timestamp_ms: i64,
        value: f64,
        start_timestamp_ms: Option<i64>,
    ) -> Self {
        Self {
            timestamp_ms,
            value,
            start_timestamp_ms,
        }
    }
}
