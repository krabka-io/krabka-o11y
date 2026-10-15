use std::io::Read as _;

use axum::http::header::CONTENT_ENCODING;
use flate2::read::GzDecoder;
use krabka_units::convert::ByteSizeExt as _;
use prost::Message as _;

use super::*;
use crate::wire::WireError;

super::ingest_push_handler!(
    clocks_push,
    clocks_push_inner,
    super::respond_ingest_request
);

pub(crate) async fn clocks_push_inner(
    state: &DistributorState,
    tenant: &TenantId,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<PushOutcome, PushError> {
    require_snappy_encoding(headers)?;
    let readings = decode_clock_readings(body, state.max_decompressed)?;
    // Stamp the receive time once for the whole request. A per-record stamp
    // would spread the decode cost of the batch across the readings and report
    // a skew that grows with the batch size.
    let ingest_unix_nanos = ingest_stamp();

    let items = readings.len() as u64;
    // Backfill the decoded reading count onto the enclosing span.
    tracing::Span::current().record("krabka.ingest.series", items);

    let appended = append_clock_readings(state, tenant, &readings, ingest_unix_nanos).await?;
    Ok(series_push_result(state, tenant, items, appended))
}

pub(crate) async fn influx_push(
    State(state): State<Arc<DistributorState>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    body: BodyBytes,
) -> Response {
    let ingest = IngestRequestStart::begin(&headers, &principal, &body);
    let result = async {
        influx_push_inner(
            &state,
            ingest.authorized_tenant()?,
            &headers,
            raw_query.as_deref(),
            &body,
        )
        .await
    }
    .instrument(ingest.span())
    .await;
    ingest.respond(&state, result)
}

pub(crate) async fn influx_push_inner(
    state: &DistributorState,
    tenant: &TenantId,
    headers: &HeaderMap,
    raw_query: Option<&str>,
    body: &[u8],
) -> Result<PushOutcome, PushError> {
    let mut headers = headers.clone();
    if headers
        .get(axum::http::header::CONTENT_ENCODING)
        .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"x-gzip"))
    {
        headers.insert(
            axum::http::header::CONTENT_ENCODING,
            axum::http::HeaderValue::from_static("gzip"),
        );
    }
    let body = decode_otlp_http_body(&headers, body, state.max_decompressed)?;
    let mut series = decode_influx(&body, raw_query, state.clock.now_unix_ms())?;
    let items = series.len() as u64;
    tracing::Span::current().record("krabka.ingest.series", items);
    let appended = append_decoded_series(state, tenant, &mut series).await?;
    Ok(series_push_result(state, tenant, items, appended))
}

super::ingest_push_handler!(otlp_push, otlp_push_inner, respond_otlp_push);

fn respond_otlp_push(
    state: &DistributorState,
    ingest: &IngestRequestStart,
    result: Result<OtlpPushOutcome, PushError>,
) -> Response {
    if let Some(metrics) = &state.metrics {
        metrics.record_ingest(IngestRequest {
            outcome: RequestOutcome::from_result(&result),
            body: ingest.body_size,
            items: result.as_ref().map_or(0, |outcome| outcome.items),
            elapsed: ingest.started.elapsed().as_time(),
        });
    }
    match result {
        Ok(outcome) => {
            let mut response = outcome.success.into_response();
            let body = ExportMetricsServiceResponse {
                partial_success: outcome.partial_success.map(|partial| {
                    ExportMetricsPartialSuccess {
                        rejected_data_points: i64::try_from(partial.rejected_data_points)
                            .unwrap_or(i64::MAX),
                        error_message: partial.error_message,
                    }
                }),
            }
            .encode_to_vec();
            *response.body_mut() = axum::body::Body::from(body.clone());
            response.headers_mut().insert(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/x-protobuf"),
            );
            response.headers_mut().insert(
                axum::http::header::CONTENT_LENGTH,
                HeaderValue::from_str(&body.len().to_string())
                    .expect("a response body length is an HTTP header value"),
            );
            response.headers_mut().insert(
                "x-content-type-options",
                HeaderValue::from_static("nosniff"),
            );
            response
        }
        Err(error) => error.into_response(),
    }
}

pub(crate) struct OtlpPartialSuccess {
    pub(crate) rejected_data_points: u64,
    pub(crate) error_message: String,
}

pub(crate) struct OtlpPushOutcome {
    pub(crate) success: PushSuccess,
    pub(crate) items: u64,
    pub(crate) partial_success: Option<OtlpPartialSuccess>,
}

pub(crate) async fn otlp_push_inner(
    state: &DistributorState,
    tenant: &TenantId,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<OtlpPushOutcome, PushError> {
    require_otlp_protobuf_content_type(headers)?;
    let body = decode_otlp_http_body(headers, body, state.max_decompressed)?;
    let strategy = otlp_translation_strategy(headers)?;
    let decoded = decode_tenant_otlp(state, tenant, strategy, |options| {
        decode_otlp_stateful_bytes_partial(&body, options)
    })?;
    let partial_success = (decoded.rejected_data_points > 0).then(|| OtlpPartialSuccess {
        rejected_data_points: decoded.rejected_data_points,
        error_message: decoded
            .error_message
            .expect("a rejected OTLP data point records its error"),
    });
    let mut series = decoded.series;
    let items = series.len() as u64;
    // Backfill the decoded series count onto the enclosing `metrics_ingest` span.
    tracing::Span::current().record("krabka.ingest.series", items);
    if !append_decoded_series(state, tenant, &mut series).await? {
        return Ok(OtlpPushOutcome {
            success: PushSuccess::Accepted { counts: None },
            items,
            partial_success,
        });
    }
    if let Some(metrics) = &state.metrics {
        metrics.record_ingest_series(tenant.as_str(), items);
    }
    Ok(OtlpPushOutcome {
        success: PushSuccess::Ok { counts: None },
        items,
        partial_success,
    })
}

super::ingest_push_handler!(push, push_inner, super::respond_ingest_request);

struct DecodedRemoteWrite {
    series: Vec<super::DecodedSeries>,
    counts: Option<WrittenCounts>,
}

pub(crate) async fn push_inner(
    state: &DistributorState,
    tenant: &TenantId,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<PushOutcome, PushError> {
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok());
    let format = negotiate(content_type)?;
    require_snappy_encoding(headers)?;

    let decoded = match format {
        WireFormat::RemoteWriteV1 => DecodedRemoteWrite {
            series: decode_v1(body, state.max_decompressed)?,
            counts: None,
        },
        WireFormat::RemoteWriteV2 => {
            let decoded = decode_v2(body, state.max_decompressed)?;
            DecodedRemoteWrite {
                series: decoded.series,
                counts: Some(decoded.counts),
            }
        }
    };
    let mut series = decoded.series;
    let counts = decoded.counts;
    let items = series.len() as u64;
    // Backfill the decoded series count onto the enclosing `metrics_ingest` span.
    tracing::Span::current().record("krabka.ingest.series", items);

    if !append_decoded_series(state, tenant, &mut series).await? {
        return Ok(PushOutcome {
            success: PushSuccess::Accepted {
                counts: counts.map(|_| WrittenCounts::default()),
            },
            items,
        });
    }

    if let Some(metrics) = &state.metrics {
        metrics.record_ingest_series(tenant.as_str(), items);
    }
    Ok(PushOutcome {
        success: PushSuccess::Ok { counts },
        items,
    })
}

pub(crate) fn decode_otlp_http_body(
    headers: &HeaderMap,
    body: &[u8],
    max_decompressed: ByteSize,
) -> Result<Vec<u8>, WireError> {
    let limit = max_decompressed.bytes_usize();
    let encoding = match headers.get(CONTENT_ENCODING) {
        None => "identity",
        Some(value) => value
            .to_str()
            .map_err(|_| WireError::UnsupportedContentEncoding("non-UTF-8".into()))?,
    };
    if encoding.eq_ignore_ascii_case("identity") {
        if body.len() > limit {
            return Err(WireError::DecodedBodyTooLarge(limit));
        }
        return Ok(body.to_vec());
    }
    if !encoding.eq_ignore_ascii_case("gzip") {
        return Err(WireError::UnsupportedContentEncoding(encoding.to_string()));
    }

    let mut decoded = Vec::new();
    GzDecoder::new(body)
        .take(u64::try_from(limit).unwrap_or(u64::MAX).saturating_add(1))
        .read_to_end(&mut decoded)
        .map_err(|error| WireError::GzipDecode(error.to_string()))?;
    if decoded.len() > limit {
        return Err(WireError::DecodedBodyTooLarge(limit));
    }
    Ok(decoded)
}
