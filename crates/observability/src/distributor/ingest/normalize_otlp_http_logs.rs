use super::{
    DistributorError, HeaderMap, Message, ProtoExportLogsServiceRequest, Time, WalLogRecord,
    decode_loki_http_body, is_protobuf_content_type, normalize_otlp_logs,
    normalize_otlp_proto_logs,
};

pub(crate) fn normalize_otlp_http_logs(
    headers: &HeaderMap,
    body: &[u8],
    reject_old_samples_max_age: Option<Time>,
    creation_grace_period: Option<Time>,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    // OTLP/HTTP clients (e.g. the OpenTelemetry SDK's otlphttp exporter, which
    // defaults to gzip) honour Content-Encoding just like the Loki push path, so
    // decompress before decode. Without this, a gzip body reaches the protobuf
    // decoder as raw deflate stream bytes and fails to parse.
    let body = decode_loki_http_body(headers, body)?;
    let body = body.as_slice();

    if is_protobuf_content_type(headers) {
        let payload =
            ProtoExportLogsServiceRequest::decode(body).map_err(DistributorError::OtlpDecode)?;
        return normalize_otlp_proto_logs(
            headers,
            payload,
            reject_old_samples_max_age,
            creation_grace_period,
        );
    }

    let payload = serde_json::from_slice(body).map_err(|_| DistributorError::InvalidOtlpPayload)?;
    normalize_otlp_logs(
        headers,
        payload,
        reject_old_samples_max_age,
        creation_grace_period,
    )
}
