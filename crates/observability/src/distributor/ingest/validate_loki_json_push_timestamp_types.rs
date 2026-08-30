use super::*;

pub(crate) fn validate_loki_json_push_timestamp_types(
    payload: &LokiTypedPushRequest,
    body: &[u8],
) -> Result<(), DistributorError> {
    for stream in &payload.streams {
        let Some(values) = &stream.values else {
            continue;
        };
        for value in values {
            let Some(timestamp) = value.get(0) else {
                continue;
            };
            if !timestamp.is_string() {
                return Err(DistributorError::InvalidJsonTimestampSyntax(
                    loki_json_timestamp_value_parse_error(body, timestamp, value.get(1)),
                ));
            }
        }
    }

    Ok(())
}
