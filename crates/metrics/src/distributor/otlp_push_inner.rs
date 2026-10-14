use super::{
    DistributorState, HeaderMap, PushError, PushSuccess, TenantId, append_decoded_series,
    decode_otlp_http_body, decode_otlp_stateful_bytes_partial, decode_tenant_otlp,
    otlp_translation_strategy, require_otlp_protobuf_content_type,
};

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
