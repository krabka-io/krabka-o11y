use super::{Status, StatusCode};

pub(crate) fn status_of(status: Option<&Status>) -> (StatusCode, String) {
    match status {
        Some(status) => (StatusCode::from_i32(status.code), status.message.clone()),
        None => (StatusCode::Unset, String::new()),
    }
}
