use super::*;

pub(crate) async fn push_handler(
    Extension(state): Extension<Arc<DistributorState>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::push::v1::PushRequest>,
) -> Result<ConnectResponse<pb::push::v1::PushResponse>, ConnectError> {
    let start = std::time::Instant::now();
    // No raw body is exposed by the Connect codec; the decoded message size is a
    // faithful proxy for the request payload bytes.
    let bytes = req.0.encoded_len() as u64;
    let tenant = tenant_from_headers(&headers, &state.tenant_policy);
    // ONE server span per ingest request (not per sample). `krabka.ingest.samples`
    // is filled in after the body runs and the item count is known.
    let ingest_span = tracing::info_span!(
        "profiles_ingest",
        otel.kind = "server",
        messaging.system = "kafka",
        messaging.destination.name = PROFILES_WAL_TOPIC,
        krabka.tenant = ingest_span_tenant(tenant.as_ref().ok()),
        krabka.ingest.samples = tracing::field::Empty,
        krabka.ingest.bytes = bytes,
    );
    let result = async {
        let tenant = tenant.as_ref().map_err(TenantResolveError::clone)?;
        // Before the profiles are decoded, so a denied push reaches no WAL.
        authorize_tenant(&principal, tenant)?;
        let raws = decode_push(&req.0, state.max_decompressed)?;
        let items = raws.len() as u64;
        process_raw(&state, tenant, raws).await?;
        Ok::<u64, ProfilesError>(items)
    }
    .instrument(ingest_span.clone())
    .await;
    let items = *result.as_ref().unwrap_or(&0);
    ingest_span.record("krabka.ingest.samples", items);
    if let Ok(tenant) = &tenant {
        state.metrics.record_ingest_samples(tenant.as_str(), items);
    }
    state.metrics.record_ingest(
        result.is_ok(),
        IngestBytes(bytes),
        IngestItems(items),
        start.elapsed().as_time(),
    );
    result.map_err(connect_error)?;
    Ok(ConnectResponse::new(pb::push::v1::PushResponse {}))
}
