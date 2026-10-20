use super::{IntoResponse, Json, LimitError, PromqlError, Response, StatusCode, WireError, json};

struct ErrorClassification {
    status: StatusCode,
    error_type: &'static str,
}

#[derive(Debug)]
pub(crate) struct ApiError {
    pub(crate) status: StatusCode,
    pub(crate) error_type: &'static str,
    pub(crate) message: String,
}

impl ApiError {
    pub(crate) fn bad_data(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            error_type: "bad_data",
            message: message.into(),
        }
    }

    pub(crate) fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            error_type: "not_found",
            message: message.into(),
        }
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            error_type: "execution",
            message: message.into(),
        }
    }

    pub(crate) fn timeout(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            error_type: "timeout",
            message: message.into(),
        }
    }

    /// Axum cancels an in-flight handler by dropping its future, so a client
    /// disconnect normally has no response channel on which to send this. This
    /// mapping is retained for cancellation sources that remain representable.
    #[allow(dead_code)]
    pub(crate) fn canceled(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::from_u16(499).expect("499 is a valid HTTP status"),
            error_type: "canceled",
            message: message.into(),
        }
    }
}

impl From<WireError> for ApiError {
    fn from(error: WireError) -> Self {
        let status = StatusCode::from_u16(error.status_code()).unwrap_or(StatusCode::BAD_REQUEST);
        Self {
            status,
            error_type: "bad_data",
            message: error.to_string(),
        }
    }
}

impl From<LimitError> for ApiError {
    fn from(error: LimitError) -> Self {
        let status = StatusCode::from_u16(error.http_status()).unwrap_or(StatusCode::BAD_REQUEST);
        Self {
            status,
            error_type: error.error_type(),
            message: error.message(),
        }
    }
}

impl From<PromqlError> for ApiError {
    fn from(error: PromqlError) -> Self {
        let classification = match &error {
            PromqlError::Parse(_) | PromqlError::Plan(_) => ErrorClassification {
                status: StatusCode::BAD_REQUEST,
                error_type: "bad_data",
            },
            PromqlError::Unsupported(_) | PromqlError::Exec(_) => ErrorClassification {
                status: StatusCode::UNPROCESSABLE_ENTITY,
                error_type: "execution",
            },
            PromqlError::Limit(limit_error) => return Self::from(limit_error.clone()),
            PromqlError::Overloaded { .. } => ErrorClassification {
                status: StatusCode::TOO_MANY_REQUESTS,
                error_type: "too_many_requests",
            },
            PromqlError::Store(_) => ErrorClassification {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                error_type: "execution",
            },
        };
        Self {
            status: classification.status,
            error_type: classification.error_type,
            message: error.to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut response = Json(json!({
            "status": "error",
            "errorType": self.error_type,
            "error": self.message,
        }))
        .into_response();
        *response.status_mut() = self.status;
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_termination_errors_match_prometheus_statuses() {
        let timeout = ApiError::timeout("deadline");
        assert2::assert!(timeout.status == StatusCode::SERVICE_UNAVAILABLE);
        assert2::assert!(timeout.error_type == "timeout");

        let canceled = ApiError::canceled("gone");
        assert2::assert!(canceled.status.as_u16() == 499);
        assert2::assert!(canceled.error_type == "canceled");
    }
}
