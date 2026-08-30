use super::{Status, StatusCode};

pub(crate) fn status_from_http_status(http_status: u16, message: String) -> Status {
    if http_status == StatusCode::TOO_MANY_REQUESTS.as_u16() {
        Status::resource_exhausted(message)
    } else if http_status == StatusCode::INTERNAL_SERVER_ERROR.as_u16() {
        Status::internal(message)
    } else {
        Status::invalid_argument(message)
    }
}
