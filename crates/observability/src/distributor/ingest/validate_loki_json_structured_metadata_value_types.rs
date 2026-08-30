use super::*;

pub(crate) fn validate_loki_json_structured_metadata_value_types(
    payload: &LokiTypedPushRequest,
    body: &[u8],
) -> Result<(), DistributorError> {
    for stream in &payload.streams {
        let Some(values) = &stream.values else {
            continue;
        };
        for value in values {
            let Some(metadata_value) = value.get(2) else {
                continue;
            };
            let Value::Object(metadata) = metadata_value else {
                return Err(DistributorError::InvalidStructuredMetadataSyntax(
                    loki_structured_metadata_object_parse_error(body, metadata_value),
                ));
            };
            if let Some((name, value)) = metadata.iter().find(|(_, value)| !value.is_string()) {
                return Err(DistributorError::InvalidStructuredMetadataSyntax(
                    loki_structured_metadata_value_parse_error(body, name, value),
                ));
            }
        }
    }

    Ok(())
}
