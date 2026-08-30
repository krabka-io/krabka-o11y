use super::StatusCode;

pub(crate) fn block_status(status: super::super::StatusCode) -> StatusCode {
    match status {
        super::super::StatusCode::Unset => StatusCode::Unset,
        super::super::StatusCode::Ok => StatusCode::Ok,
        super::super::StatusCode::Error => StatusCode::Error,
    }
}
