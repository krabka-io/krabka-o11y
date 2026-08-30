use super::{DistributorState, HeaderMap, PushSuccess, PushError, tenant_from_headers, negotiate, require_snappy_encoding, WireFormat, decode_v1, decode_v2, append_decoded_series, WrittenCounts};

pub(crate) async fn push_inner(
    state: &DistributorState,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<(PushSuccess, u64), PushError> {
    let tenant = tenant_from_headers(headers)?;
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok());
    let format = negotiate(content_type)?;
    require_snappy_encoding(headers)?;

    let (mut series, counts) = match format {
        WireFormat::RemoteWriteV1 => (decode_v1(body, state.max_decompressed)?, None),
        WireFormat::RemoteWriteV2 => {
            let (series, counts) = decode_v2(body, state.max_decompressed)?;
            (series, Some(counts))
        }
    };
    let items = series.len() as u64;
    // Backfill the decoded series count onto the enclosing `metrics_ingest` span.
    tracing::Span::current().record("krabka.ingest.series", items);

    if !append_decoded_series(state, tenant, &mut series).await? {
        return Ok((
            PushSuccess::Accepted {
                counts: counts.map(|_| WrittenCounts::default()),
            },
            items,
        ));
    }

    if let Some(metrics) = &state.metrics {
        metrics.record_ingest_series(tenant, items);
    }
    Ok((PushSuccess::NoContent { counts }, items))
}
