use super::*;

pub(crate) async fn otlp_http_handler(
    Extension(state): Extension<Arc<DistributorState>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let start = std::time::Instant::now();
    let bytes = body.len() as u64;
    let mut items: u64 = 0;
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
        let body = decode_http_body(&headers, &body, state.max_decompressed)?;
        let json = is_json(&headers)?;
        let req = if json {
            serde_json::from_slice(&body)
                .map_err(|err| ProfilesError::Decode(format!("OTLP profiles JSON decode: {err}")))?
        } else {
            pb::otlp_profiles::ExportProfilesServiceRequest::decode(body.as_slice())
                .map_err(|err| ProfilesError::Decode(format!("OTLP profiles decode: {err}")))?
        };
        let raws = decode_otlp(&req)?;
        items = raws.len() as u64;
        process_raw(&state, tenant, raws).await?;
        let response = pb::otlp_profiles::ExportProfilesServiceResponse {
            partial_success: None,
        };
        if json {
            serde_json::to_vec(&response)
                .map(|body| ("application/json", body))
                .map_err(|err| ProfilesError::Internal(format!("OTLP profiles JSON encode: {err}")))
        } else {
            Ok(("application/x-protobuf", response.encode_to_vec()))
        }
    }
    .instrument(ingest_span.clone())
    .await;

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
    match result {
        Ok((content_type, body)) => (
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, content_type)],
            Bytes::from(body),
        )
            .into_response(),
        Err(err) => profiles_error_response(err),
    }
}

fn is_json(headers: &HeaderMap) -> Result<bool, ProfilesError> {
    let content_type = match headers.get(axum::http::header::CONTENT_TYPE) {
        Some(value) => value
            .to_str()
            .map_err(|_| ProfilesError::UnsupportedFormat("non-ASCII content-type".into()))?,
        None => "application/x-protobuf",
    }
    .split(';')
    .next()
    .unwrap_or_default()
    .trim();
    if content_type.eq_ignore_ascii_case("application/json") {
        Ok(true)
    } else if content_type.eq_ignore_ascii_case("application/protobuf")
        || content_type.eq_ignore_ascii_case("application/x-protobuf")
    {
        Ok(false)
    } else {
        Err(ProfilesError::UnsupportedFormat(content_type.to_string()))
    }
}

fn decode_http_body(
    headers: &HeaderMap,
    body: &[u8],
    max_output: ByteSize,
) -> Result<Vec<u8>, ProfilesError> {
    let encoding = match headers.get(axum::http::header::CONTENT_ENCODING) {
        Some(value) => value
            .to_str()
            .map_err(|_| ProfilesError::UnsupportedFormat("non-ASCII content-encoding".into()))?,
        None => "identity",
    };
    if encoding.eq_ignore_ascii_case("gzip") {
        gunzip(body, max_output)
    } else if encoding.eq_ignore_ascii_case("identity") {
        Ok(body.to_vec())
    } else {
        Err(ProfilesError::UnsupportedFormat(format!(
            "content-encoding {encoding}"
        )))
    }
}
