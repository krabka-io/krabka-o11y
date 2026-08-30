use super::*;

/// One sample's WAL payload.
///
/// A clock reading carries far more fields than any other payload, so it lives
/// behind a box. Without one the enum would be as wide as its widest variant,
/// and every float sample in the WAL would pay for the clock fields it does not
/// hold.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SamplePayload {
    Float {
        timestamp_ms: i64,
        value: f64,
        start_timestamp_ms: Option<i64>,
    },
    Hist {
        timestamp_ms: i64,
        hist: NativeHistogram,
    },
    ClockReading(Box<ClockReadingPayload>),
    Metadata {
        metric_family_name: String,
        metric_type: String,
        help: String,
        unit: String,
    },
    Exemplars,
}

impl SamplePayload {
    /// The block timestamp this payload sorts and indexes by, in epoch
    /// milliseconds. A payload that carries no sample, such as metadata, has
    /// none.
    #[must_use]
    pub fn timestamp_ms(&self) -> Option<i64> {
        match self {
            Self::Float { timestamp_ms, .. } | Self::Hist { timestamp_ms, .. } => {
                Some(*timestamp_ms)
            }
            Self::ClockReading(payload) => Some(payload.timestamp_ms()),
            Self::Metadata { .. } | Self::Exemplars => None,
        }
    }
}
