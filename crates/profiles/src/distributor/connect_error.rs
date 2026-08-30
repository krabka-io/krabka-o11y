use super::*;

pub(crate) fn connect_error(err: ProfilesError) -> ConnectError {
    if let ProfilesError::Limit(limit) = &err {
        return ConnectError::new(limit_connect_code(limit), err.to_string());
    }
    let code = match err.status_code() {
        400 | 415 => Code::InvalidArgument,
        _ => Code::Internal,
    };
    let message = client_facing_message(&err);
    drop(err);
    ConnectError::new(code, message)
}
