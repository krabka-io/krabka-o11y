use super::{
    ClockWireError, IntoResponse, LimitError, OtlpError, ProduceError, RequestTenantError,
    Response, StatusCode, TenantAccessError, TenantDenied, WalBatchError, WireError,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum PushError {
    /// The request names no usable tenant.
    ///
    /// Its response is Mimir's plain-text tenant rejection, not the status
    /// and message of the other variants.
    #[error(transparent)]
    Tenant(#[from] RequestTenantError),
    /// The request's principal is not granted its tenant.
    ///
    /// Its response is the 403 of [`TenantDenied`].
    #[error(transparent)]
    Denied(#[from] TenantDenied),
    /// The authentication layer put no principal on the request.
    ///
    /// Every served router has that layer, so this is a server fault, and the
    /// request fails closed.
    #[error("the request has no principal: the server did not authenticate it")]
    MissingPrincipal,
    #[error(
        "too-old-sample: timestamp {timestamp_ms} is older than oldest allowed {oldest_allowed_ms}"
    )]
    TooOldSample {
        timestamp_ms: i64,
        oldest_allowed_ms: i64,
    },
    #[error(
        "too-far-in-future: timestamp {timestamp_ms} is newer than newest allowed {newest_allowed_ms}"
    )]
    TooFarInFuture {
        timestamp_ms: i64,
        newest_allowed_ms: i64,
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
    /// A WAL batch that appended in part or not at all.
    ///
    /// This is separate from [`Self::Produce`] because it carries how much of
    /// the batch reached the broker. The status stays 500, so a Prometheus
    /// sender retries the whole request, and the metrics read path drops the
    /// duplicate samples that the retry writes.
    #[error(transparent)]
    ProduceBatch(#[from] WalBatchError<ProduceError>),
}

impl From<TenantAccessError> for PushError {
    fn from(error: TenantAccessError) -> Self {
        match error {
            TenantAccessError::Unresolved(error) => Self::Tenant(error),
            TenantAccessError::Denied(error) => Self::Denied(error),
        }
    }
}

impl IntoResponse for PushError {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::Tenant(error) => return error.clone().into_response(),
            Self::Denied(error) => return error.clone().into_response(),
            Self::Limit(error) => {
                StatusCode::from_u16(error.http_status()).unwrap_or(StatusCode::BAD_REQUEST)
            }
            Self::TooOldSample { .. } | Self::TooFarInFuture { .. } => StatusCode::BAD_REQUEST,
            Self::Wire(error) => StatusCode::from_u16(error.status_code())
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            Self::Clock(error) => {
                StatusCode::from_u16(error.status_code()).unwrap_or(StatusCode::BAD_REQUEST)
            }
            Self::Otlp(error) => {
                StatusCode::from_u16(error.status_code()).unwrap_or(StatusCode::BAD_REQUEST)
            }
            Self::MissingPrincipal | Self::Produce(_) | Self::ProduceBatch(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };
        (status, self.to_string()).into_response()
    }
}
