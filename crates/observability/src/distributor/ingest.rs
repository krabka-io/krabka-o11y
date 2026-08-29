#[allow(clippy::wildcard_imports)]
use super::*;

/// Records one push-handler ingest outcome from the response status and returns
/// the response unchanged.
///
/// `ok` is true for any 2xx. The WAL/produce failure counter is bumped
/// separately at the [`append_distributor_wal_records`] error site, so a 4xx
/// validation or quota reject here does not inflate it.
pub(crate) fn record_ingest_response(
    state: &DistributorState,
    resp: Response,
    body: ByteSize,
    items: u64,
    start: Instant,
) -> Response {
    let ok = resp.status().is_success();
    state
        .metrics
        .record_ingest(ok, body, items, start.elapsed().as_time());
    resp
}

/// A measured length, as a byte quantity.
pub(crate) fn measured_size(len: usize) -> ByteSize {
    ByteSize::from_bytes(u64::try_from(len).unwrap_or(u64::MAX))
}

pub(crate) fn otlp_http_error_response(error: DistributorError) -> Response {
    if matches!(
        error,
        DistributorError::TimestampTooOld { .. } | DistributorError::TimestampTooNew { .. }
    ) {
        return (
            StatusCode::BAD_REQUEST,
            [("content-type", "application/x-protobuf")],
            encode_otlp_status_message(&error.to_string()),
        )
            .into_response();
    }

    error.into_response()
}

pub(crate) fn encode_otlp_status_message(message: &str) -> Vec<u8> {
    let message = message.trim_end_matches('\n').as_bytes();
    let mut body = vec![0x12];
    encode_varint(message.len() as u64, &mut body);
    body.extend_from_slice(message);
    body
}

pub(crate) fn encode_varint(mut value: u64, body: &mut Vec<u8>) {
    while value >= 0x80 {
        // `|` against `^` is a permanent mutation survivor here: the masked
        // byte has its top bit clear, so setting it and flipping it agree.
        body.push(u8::try_from(value & 0x7f).expect("masked varint byte fits in u8") | 0x80);
        value >>= 7;
    }
    body.push(u8::try_from(value).expect("final varint byte fits in u8"));
}

pub(crate) fn validate_ingest_body_limit(
    state: &DistributorState,
    body: ByteSize,
) -> Result<(), DistributorError> {
    let Some(max) = state.max_ingest_body else {
        return Ok(());
    };
    if body > max {
        // The error carries plain integers so its rendered message is fixed by
        // the `#[error]` format string alone.
        return Err(DistributorError::IngestBodyTooLarge {
            body_bytes: body.bytes_usize(),
            max_bytes: max.bytes_usize(),
        });
    }
    Ok(())
}

pub(crate) async fn append_wal_records(
    sink: &dyn LogWalSink,
    records: Vec<WalLogRecord>,
) -> Result<(), WalSinkError> {
    for record in records {
        sink.append(record).await?;
    }
    Ok(())
}

pub(crate) async fn append_distributor_wal_records(
    state: &DistributorState,
    records: Vec<WalLogRecord>,
) -> Result<(), DistributorError> {
    // A quota/rate-limit reject is a 4xx client error, NOT a WAL-append
    // failure, so it must not bump the WAL failure counter.
    check_ingest_quota(state.ingest_limiter.as_ref(), &records).await?;
    let result = if let Some(timeout) = state.wal_append_timeout {
        match tokio::time::timeout(
            timeout.to_std(),
            append_wal_records(state.sink.as_ref(), records),
        )
        .await
        {
            Ok(inner) => inner.map_err(DistributorError::from),
            Err(_) => Err(DistributorError::WalAppendTimeout),
        }
    } else {
        append_wal_records(state.sink.as_ref(), records)
            .await
            .map_err(DistributorError::from)
    };
    // Bump the WAL/produce append-failure counter only at the actual append
    // error site (timeout or sink error), never on a 4xx validation/quota
    // reject handled above or upstream.
    if result.is_err() {
        state.metrics.record_wal_append_failure();
    }
    result
}

pub(crate) async fn check_ingest_quota(
    limiter: &dyn LogIngestLimiter,
    records: &[WalLogRecord],
) -> Result<(), DistributorError> {
    let Some(first) = records.first() else {
        return Ok(());
    };
    limiter
        .check(&first.tenant, records)
        .await
        .map_err(DistributorError::IngestQuota)
}

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

pub(crate) fn validate_loki_json_push_value_arrays(
    payload: &LokiTypedPushRequest,
    body: &[u8],
) -> Result<(), DistributorError> {
    for stream in &payload.streams {
        let Some(values) = &stream.values else {
            continue;
        };
        for value in values {
            if !value.is_array() {
                return Err(DistributorError::InvalidJsonPushValueSyntax(
                    loki_json_push_value_parse_error(body, value),
                ));
            }
        }
    }

    Ok(())
}

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

pub(crate) fn loki_json_push_value_parse_error(body: &[u8], value: &Value) -> String {
    let body = String::from_utf8_lossy(body);
    let value_text = value.to_string();
    let value_start = body.find(&value_text).unwrap_or(body.len());
    let context = loki_decode_error_context(&body, value_start.saturating_add(10));
    let bigger_context = loki_decode_error_context(&body, value_start.saturating_sub(30));

    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Unknown value type, error found in #10 byte of ...|{context}|..., bigger context ...|{bigger_context}|...\n"
    )
}

pub(crate) fn loki_json_push_payload_parse_error(body: &[u8]) -> String {
    let body = String::from_utf8_lossy(body);
    let value_start = body
        .char_indices()
        .find_map(|(index, char)| (!char.is_whitespace()).then_some(index))
        .unwrap_or(body.len());
    let found = body[value_start..].chars().next().unwrap_or('\0');
    let context_start = previous_char_boundary(&body, value_start);
    let context_end = previous_char_boundary(&body, body.len().min(context_start + 11));
    let context = &body[context_start..context_end];
    let bigger_context = loki_decode_error_context(&body, value_start);

    format!(
        "readObjectStart: expect {{ or n, but found {found}, error found in #1 byte of ...|{context}|..., bigger context ...|{bigger_context}|...\n"
    )
}

pub(crate) fn loki_json_push_values_field_parse_error(body: &[u8], value: &Value) -> String {
    let body = String::from_utf8_lossy(body);
    let value_text = value.to_string();
    let value_start = body.find(&value_text).unwrap_or(body.len());
    let context = loki_decode_error_context(&body, value_start.saturating_add(3));
    let bigger_context = loki_decode_error_context(&body, value_start.saturating_sub(37));

    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Unknown value type, error found in #10 byte of ...|{context}|..., bigger context ...|{bigger_context}|...\n"
    )
}

pub(crate) fn loki_json_push_stream_parse_error(body: &[u8], value: &Value) -> String {
    let body = String::from_utf8_lossy(body);
    let value_text = value.to_string();
    let value_start = body.find(&value_text).unwrap_or(body.len());
    let context = loki_decode_error_context(&body, value_start.saturating_add(4));
    let bigger_context = loki_decode_error_context(&body, value_start.saturating_sub(12));

    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like object, but can't find closing '}}' symbol, error found in #10 byte of ...|{context}|..., bigger context ...|{bigger_context}|...\n"
    )
}

pub(crate) fn loki_json_push_labels_field_parse_error(body: &[u8]) -> String {
    let body = String::from_utf8_lossy(body);
    let context = loki_decode_error_context(&body, body.len().saturating_sub(12));
    let bigger_context = loki_decode_error_context(&body, body.len().saturating_sub(52));

    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like object, but can't find closing '}}' symbol, error found in #10 byte of ...|{context}|..., bigger context ...|{bigger_context}|...\n"
    )
}

pub(crate) fn loki_json_push_streams_parse_error(body: &[u8], value: &Value) -> String {
    let body = String::from_utf8_lossy(body);
    let value_text = value.to_string();
    let value_start = body.find(&value_text).unwrap_or(body.len());
    let context_start = previous_char_boundary(&body, value_start.saturating_sub(9));
    let context_end = previous_char_boundary(&body, body.len().min(context_start + 20));
    let context = &body[context_start..context_end];
    let bigger_context = loki_decode_error_context(&body, value_start.saturating_sub(11));

    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: decode slice: expect [ or n, but found \", error found in #10 byte of ...|{context}|..., bigger context ...|{bigger_context}|...\n"
    )
}

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

pub(crate) fn loki_structured_metadata_object_parse_error(body: &[u8], value: &Value) -> String {
    let body = String::from_utf8_lossy(body);
    let value_text = value.to_string();
    let value_start = body.find(&value_text).unwrap_or(body.len());
    let context = loki_decode_error_context(&body, value_start.saturating_sub(3));
    let bigger_context = loki_decode_error_context(&body, value_start.saturating_sub(43));

    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like object, but can't find closing '}}' symbol, error found in #10 byte of ...|{context}|..., bigger context ...|{bigger_context}|...\n"
    )
}

pub(crate) fn loki_structured_metadata_value_parse_error(
    body: &[u8],
    name: &str,
    value: &Value,
) -> String {
    let body = String::from_utf8_lossy(body);
    let key = quote_logql_string(name);
    let needle = format!("{key}:{value}");
    let value_start = body.find(&needle).map_or_else(
        || body.find(&value.to_string()).unwrap_or(body.len()),
        |offset| offset + key.len() + 1,
    );
    let context = loki_decode_error_context(&body, value_start.saturating_sub(3));
    let bigger_context = loki_decode_error_context(&body, value_start.saturating_sub(43));

    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value is string, but can't find closing '\"' symbol, error found in #10 byte of ...|{context}|..., bigger context ...|{bigger_context}|...\n"
    )
}

pub(crate) fn loki_decode_error_context(body: &str, start: usize) -> &str {
    let start = previous_char_boundary(body, start.min(body.len()));
    let end = previous_char_boundary(body, body.len().min(start + 80));
    &body[start..end]
}

pub(crate) fn previous_char_boundary(value: &str, mut offset: usize) -> usize {
    while !value.is_char_boundary(offset) {
        offset -= 1;
    }
    offset
}

pub(crate) fn decode_loki_http_body(
    headers: &HeaderMap,
    body: &[u8],
) -> Result<Vec<u8>, DistributorError> {
    let Some(encoding) = headers
        .get(CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
    else {
        return Ok(body.to_vec());
    };
    let encoding = encoding.trim();

    if encoding.is_empty() || encoding.eq_ignore_ascii_case("snappy") {
        return Ok(body.to_vec());
    } else if encoding.eq_ignore_ascii_case("gzip") {
        let mut decoder = GzDecoder::new(body);
        let mut decompressed = Vec::new();
        decoder
            .read_to_end(&mut decompressed)
            .map_err(DistributorError::LokiGzipDecode)?;
        return Ok(decompressed);
    } else if encoding.eq_ignore_ascii_case("deflate") {
        let mut decoder = DeflateDecoder::new(body);
        let mut decompressed = Vec::new();
        decoder
            .read_to_end(&mut decompressed)
            .map_err(DistributorError::LokiDeflateDecode)?;
        return Ok(decompressed);
    }

    Err(DistributorError::UnsupportedLokiContentEncoding(
        encoding.to_string(),
    ))
}

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
