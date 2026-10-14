use super::{DistributorError, LokiTypedPushRequest, loki_json_timestamp_value_parse_error};

pub(crate) fn validate_loki_json_push_timestamp_types(
    payload: &LokiTypedPushRequest,
    body: &[u8],
) -> Result<(), DistributorError> {
    super::validate_typed_loki_values(payload, |value| {
        let Some(timestamp) = value.get(0) else {
            return Ok(());
        };
        if !timestamp.is_string() {
            return Err(DistributorError::InvalidJsonTimestampSyntax(
                loki_json_timestamp_value_parse_error(body, timestamp, value.get(1)),
            ));
        }
        Ok(())
    })
}
