use super::{
    DistributorState, HeaderMap, PushError, PushOutcome, PushSuccess, TenantId, WireFormat,
    WrittenCounts, append_decoded_series, decode_v1, decode_v2, negotiate, require_snappy_encoding,
};

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
