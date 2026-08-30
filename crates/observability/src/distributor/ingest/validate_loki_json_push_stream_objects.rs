use super::*;

pub(crate) fn validate_loki_json_push_stream_objects(
    payload: LokiPushRequest,
    body: &[u8],
) -> Result<LokiTypedPushRequest, DistributorError> {
    let Some(streams) = payload.streams else {
        return Err(DistributorError::NoValidStreams);
    };
    let Some(raw_streams) = streams.as_array() else {
        return Err(DistributorError::InvalidJsonPushValueSyntax(
            loki_json_push_streams_parse_error(body, &streams),
        ));
    };
    if raw_streams.is_empty() {
        return Err(DistributorError::NoValidStreams);
    }
    let mut streams = Vec::with_capacity(raw_streams.len());
    for stream in raw_streams {
        if !stream.is_object() {
            return Err(DistributorError::InvalidJsonPushValueSyntax(
                loki_json_push_stream_parse_error(body, stream),
            ));
        }
        if let Some(labels) = stream.get("stream")
            && !labels.is_object()
        {
            return Err(DistributorError::InvalidJsonPushValueSyntax(
                loki_json_push_labels_field_parse_error(body),
            ));
        }
        if let Some(values) = stream.get("values")
            && !values.is_array()
            && !values.is_null()
        {
            return Err(DistributorError::InvalidJsonPushValueSyntax(
                loki_json_push_values_field_parse_error(body, values),
            ));
        }
        let stream = serde_json::from_value(stream.clone())
            .map_err(|_| DistributorError::InvalidPushPayload)?;
        streams.push(stream);
    }

    Ok(LokiTypedPushRequest { streams })
}
