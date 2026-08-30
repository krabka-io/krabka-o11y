use super::*;

pub(crate) fn distributor_error_to_grpc_status(error: &DistributorError) -> tonic::Status {
    let message = error.to_string();
    match error {
        DistributorError::IngestBodyTooLarge { .. }
        | DistributorError::IngestQuota(IngestLimitError::RateLimited { .. }) => {
            tonic::Status::resource_exhausted(message)
        }
        DistributorError::IngestQuota(IngestLimitError::Unauthorized { .. }) => {
            tonic::Status::permission_denied(message)
        }
        DistributorError::IngestQuota(IngestLimitError::Unavailable { .. })
        | DistributorError::WalAppendTimeout
        | DistributorError::WalSink(_) => tonic::Status::unavailable(message),
        DistributorError::EmptyStreamLabels
        | DistributorError::InvalidOtlpAttribute
        | DistributorError::InvalidOtlpPayload
        | DistributorError::InvalidPushLabels
        | DistributorError::InvalidJsonLineSyntax(_)
        | DistributorError::InvalidJsonTimestampSyntax(_)
        | DistributorError::InvalidPushLabelSyntax(_)
        | DistributorError::InvalidPushPayload
        | DistributorError::InvalidPushValue
        | DistributorError::NoValidStreams
        | DistributorError::InvalidJsonPushValueSyntax(_)
        | DistributorError::InvalidStructuredMetadata
        | DistributorError::InvalidStructuredMetadataSyntax(_)
        | DistributorError::InvalidTimestamp
        | DistributorError::TimestampTooOld { .. }
        | DistributorError::TimestampTooOldString { .. }
        | DistributorError::TimestampTooNew { .. }
        | DistributorError::Http(_)
        | DistributorError::LokiDecode(_)
        | DistributorError::LokiDeflateDecode(_)
        | DistributorError::LokiGzipDecode(_)
        | DistributorError::LokiSnappyDecode(_)
        | DistributorError::InvalidLokiContentType(_)
        | DistributorError::UnsupportedLokiContentEncoding(_)
        | DistributorError::OtlpDecode(_) => tonic::Status::invalid_argument(message),
    }
}
