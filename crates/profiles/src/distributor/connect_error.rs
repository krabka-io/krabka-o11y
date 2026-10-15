use super::{Code, ConnectError, ProfilesError};

/// Returns the client-facing message for `err`.
///
/// Input faults expose their specific message. Internal faults log the detail
/// on the server and return a generic message to the caller.
pub(crate) fn client_facing_message(err: &ProfilesError) -> String {
    if err.status_code() >= 500 {
        tracing::error!(error = %err, "profiles distributor internal error");
        INTERNAL_ERROR_MESSAGE.to_string()
    } else {
        err.to_string()
    }
}

pub(crate) const INTERNAL_ERROR_MESSAGE: &str = "internal server error";

pub(crate) fn limit_connect_code(err: &crate::limits::LimitError) -> Code {
    match err.connect_code() {
        "resource_exhausted" => Code::ResourceExhausted,
        "invalid_argument" => Code::InvalidArgument,
        _ => Code::Internal,
    }
}

pub(crate) fn connect_error(err: ProfilesError) -> ConnectError {
    if let ProfilesError::Limit(limit) = &err {
        return ConnectError::new(limit_connect_code(limit), err.to_string());
    }
    let code = match err.status_code() {
        400 | 415 => Code::InvalidArgument,
        403 => Code::PermissionDenied,
        _ => Code::Internal,
    };
    let message = client_facing_message(&err);
    drop(err);
    ConnectError::new(code, message)
}
