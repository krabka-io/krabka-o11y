use super::{pb, ApiError, StatusCode};

pub(crate) fn require_remote_read_samples_response(request: &pb::v1::ReadRequest) -> Result<(), ApiError> {
    if request.accepted_response_types.is_empty()
        || request
            .accepted_response_types
            .contains(&(pb::v1::ResponseType::Samples as i32))
    {
        return Ok(());
    }
    Err(ApiError {
        status: StatusCode::UNPROCESSABLE_ENTITY,
        error_type: "execution",
        message: "remote_read only supports samples responses".into(),
    })
}
