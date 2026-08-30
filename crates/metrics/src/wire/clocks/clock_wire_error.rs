use super::{WireError, i64};

/// Errors raised while decoding a clock reading batch.
#[derive(Debug, thiserror::Error)]
pub enum ClockWireError {
    /// The snappy frame or the protobuf body did not decode.
    #[error(transparent)]
    Wire(#[from] WireError),

    /// A reading named no host or no clock, so nothing identifies the series.
    #[error("clock reading {index} has an empty `{field}`")]
    EmptyIdentity { index: usize, field: &'static str },

    /// An uncertainty is a half-width and can never be negative.
    #[error("clock reading {index} has a negative uncertainty of {uncertainty_nanos}ns")]
    NegativeUncertainty {
        index: usize,
        uncertainty_nanos: i64,
    },

    /// A reading so far in the future that it would poison the per-series
    /// out-of-order window downstream.
    #[error("clock reading {index} at {reading_unix_nanos}ns is too far in the future")]
    ReadingTooFarInFuture {
        index: usize,
        reading_unix_nanos: i64,
    },

    /// The agent left a required enum at its `*_UNSPECIFIED` zero value.
    #[error("clock reading {index} leaves `{field}` unspecified")]
    UnspecifiedEnum { index: usize, field: &'static str },

    /// The agent sent a discriminant this build does not know.
    #[error("clock reading {index} has an unknown `{field}` discriminant {value}")]
    UnknownEnum {
        index: usize,
        field: &'static str,
        value: i32,
    },
}

impl ClockWireError {
    /// HTTP status code for the clock ingest endpoint.
    #[must_use]
    pub fn status_code(&self) -> u16 {
        match self {
            Self::Wire(error) => error.status_code(),
            Self::EmptyIdentity { .. }
            | Self::NegativeUncertainty { .. }
            | Self::ReadingTooFarInFuture { .. }
            | Self::UnspecifiedEnum { .. }
            | Self::UnknownEnum { .. } => 400,
        }
    }
}
