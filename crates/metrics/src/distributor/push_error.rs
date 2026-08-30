use super::{LimitError, WireError, ClockWireError, OtlpError, ProduceError, IntoResponse, Response, StatusCode};

#[derive(Debug, thiserror::Error)]
pub(crate) enum PushError {
    #[error("missing X-Scope-OrgID tenant header")]
    MissingTenant,
    #[error("invalid tenant: {0}")]
    InvalidTenant(String),
    #[error(
        "too-old-sample: timestamp {timestamp_ms} is older than oldest allowed {oldest_allowed_ms}"
    )]
    TooOldSample {
        timestamp_ms: i64,
        oldest_allowed_ms: i64,
    },
    #[error(transparent)]
    Limit(#[from] LimitError),
    #[error(transparent)]
    Wire(#[from] WireError),
    #[error(transparent)]
    Clock(#[from] ClockWireError),
    #[error(transparent)]
    Otlp(#[from] OtlpError),
    #[error(transparent)]
    Produce(#[from] ProduceError),
}

impl IntoResponse for PushError {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::Limit(error) => {
                StatusCode::from_u16(error.http_status()).unwrap_or(StatusCode::BAD_REQUEST)
            }
            Self::MissingTenant | Self::InvalidTenant(_) | Self::TooOldSample { .. } => {
                StatusCode::BAD_REQUEST
            }
            Self::Wire(error) => StatusCode::from_u16(error.status_code())
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            Self::Clock(error) => {
                StatusCode::from_u16(error.status_code()).unwrap_or(StatusCode::BAD_REQUEST)
            }
            Self::Otlp(error) => {
                StatusCode::from_u16(error.status_code()).unwrap_or(StatusCode::BAD_REQUEST)
            }
            Self::Produce(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, self.to_string()).into_response()
    }
}
