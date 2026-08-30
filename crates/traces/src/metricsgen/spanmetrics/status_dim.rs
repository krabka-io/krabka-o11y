use super::*;

pub(crate) fn status_dim(status: StatusCode) -> &'static str {
    match status {
        StatusCode::Unset => "STATUS_CODE_UNSET",
        StatusCode::Ok => "STATUS_CODE_OK",
        StatusCode::Error => "STATUS_CODE_ERROR",
    }
}
