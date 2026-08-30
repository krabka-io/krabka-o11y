use super::{json, StatusCode, WireError, LimitError, PromqlError, IntoResponse, Response, Json};

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
        let (status, error_type) = match &error {
            PromqlError::Parse(_) | PromqlError::Plan(_) => (StatusCode::BAD_REQUEST, "bad_data"),
            PromqlError::Unsupported(_) => (StatusCode::UNPROCESSABLE_ENTITY, "execution"),
            PromqlError::Exec(message) if message.starts_with("query exceeds max_samples=") => {
                (StatusCode::UNPROCESSABLE_ENTITY, "execution")
            }
            PromqlError::Exec(_) | PromqlError::Store(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "execution")
            }
        };
        Self {
            status,
            error_type,
            message: error.to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({
                "status": "error",
                "errorType": self.error_type,
                "error": self.message,
            })),
        )
            .into_response()
    }
}
