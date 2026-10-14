use super::{
    DistributorError, LokiTypedPushRequest, Value, loki_structured_metadata_object_parse_error,
    loki_structured_metadata_value_parse_error,
};

pub(crate) fn validate_loki_json_structured_metadata_value_types(
    payload: &LokiTypedPushRequest,
    body: &[u8],
) -> Result<(), DistributorError> {
    super::validate_typed_loki_values(payload, |value| {
        let Some(metadata_value) = value.get(2) else {
            return Ok(());
        };
        let Value::Object(metadata) = metadata_value else {
            return Err(DistributorError::InvalidStructuredMetadataSyntax(
                loki_structured_metadata_object_parse_error(body, metadata_value),
            ));
        };
        if let Some(entry) = metadata.iter().find(|entry| !entry.1.is_string()) {
            return Err(DistributorError::InvalidStructuredMetadataSyntax(
                loki_structured_metadata_value_parse_error(body, entry.0, entry.1),
            ));
        }
        Ok(())
    })
}
