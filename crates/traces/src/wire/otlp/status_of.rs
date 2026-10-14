use super::{Status, StatusCode, WireSpanStatus};

pub(crate) fn status_of(status: Option<&Status>) -> WireSpanStatus {
    match status {
        Some(status) => WireSpanStatus {
            code: StatusCode::from_i32(status.code),
            message: status.message.clone(),
        },
        None => WireSpanStatus {
            code: StatusCode::Unset,
            message: String::new(),
        },
    }
}
