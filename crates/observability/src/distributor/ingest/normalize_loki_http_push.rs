use super::{
    DistributorError, HeaderMap, LokiProtoPushRequest, Message, SnappyDecoder, Time, Value,
    WalLogRecord, decode_loki_http_body, is_loki_json_content_type,
    loki_json_push_payload_parse_error, normalize_loki_proto_push, normalize_loki_push,
    validate_loki_json_push_stream_objects, validate_loki_json_push_timestamp_types,
    validate_loki_json_push_value_arrays, validate_loki_json_structured_metadata_value_types,
};

pub(crate) fn normalize_loki_http_push(
    headers: &HeaderMap,
    body: &[u8],
    reject_old_samples_max_age: Option<Time>,
    creation_grace_period: Option<Time>,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let body = decode_loki_http_body(headers, body)?;
    if is_loki_json_content_type(headers)? {
        let raw_payload: Value =
            serde_json::from_slice(&body).map_err(|_| DistributorError::InvalidPushPayload)?;
        if raw_payload.is_null() {
            return Err(DistributorError::NoValidStreams);
        }
        if !raw_payload.is_object() {
            return Err(DistributorError::InvalidJsonPushValueSyntax(
                loki_json_push_payload_parse_error(&body),
            ));
        }
        let payload =
            serde_json::from_slice(&body).map_err(|_| DistributorError::InvalidPushPayload)?;
        let payload = validate_loki_json_push_stream_objects(payload, &body)?;
        validate_loki_json_push_value_arrays(&payload, &body)?;
        validate_loki_json_push_timestamp_types(&payload, &body)?;
        validate_loki_json_structured_metadata_value_types(&payload, &body)?;
        normalize_loki_push(
            headers,
            payload,
            reject_old_samples_max_age,
            creation_grace_period,
        )
    } else {
        let decompressed = SnappyDecoder::new()
            .decompress_vec(&body)
            .map_err(DistributorError::LokiSnappyDecode)?;
        let payload = LokiProtoPushRequest::decode(decompressed.as_slice())
            .map_err(DistributorError::LokiDecode)?;
        normalize_loki_proto_push(
            headers,
            payload,
            reject_old_samples_max_age,
            creation_grace_period,
        )
    }
}
