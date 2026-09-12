use super::{ApiError, StatusCode, negotiate_read_response_type, pb};

pub(crate) fn negotiate_remote_read_response_type(
    request: &pb::v1::ReadRequest,
) -> Result<pb::v1::ResponseType, ApiError> {
    negotiate_read_response_type(&request.accepted_response_types).map_err(|error| ApiError {
        status: StatusCode::UNPROCESSABLE_ENTITY,
        error_type: "execution",
        message: error.to_string(),
    })
}
