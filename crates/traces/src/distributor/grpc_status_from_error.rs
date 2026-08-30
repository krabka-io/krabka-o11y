use super::{TracesError, GrpcStatus};

pub(crate) fn grpc_status_from_error(err: &TracesError) -> GrpcStatus {
    match err {
        TracesError::Limit(_) | TracesError::RateLimit(_) => {
            GrpcStatus::resource_exhausted(err.to_string())
        }
        TracesError::Invalid(_) | TracesError::Decode(_) | TracesError::TooLarge { .. } => {
            GrpcStatus::invalid_argument(err.to_string())
        }
        TracesError::UnsupportedContentType(_) => GrpcStatus::unimplemented(err.to_string()),
        TracesError::Wal(_) | TracesError::Produce(_) | TracesError::Block(_) => {
            GrpcStatus::internal(err.to_string())
        }
    }
}
