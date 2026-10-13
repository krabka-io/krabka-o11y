use super::{
    DistributorState, HeaderMap, PushError, PushOutcome, TenantId, append_decoded_series,
    decode_influx, decode_otlp_http_body, series_push_result,
};

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
