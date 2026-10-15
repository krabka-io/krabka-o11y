use std::str::FromStr as _;

use krabka_units::convert::ByteSizeExt as _;

use super::*;

pub(crate) fn decode_tenant_otlp<T>(
    state: &DistributorState,
    tenant: &TenantId,
    strategy: TranslationStrategy,
    decode: impl for<'a> FnOnce(OtlpDecodeOptions<'a>) -> Result<T, OtlpError>,
) -> Result<T, PushError> {
    let limits = state.limits_for_tenant(tenant);
    let now = state.clock.now();
    let accumulator = state.otlp_delta_accumulators.for_tenant(tenant);
    let mut guard = accumulator.lock().expect("otlp delta accumulator poisoned");
    guard.seen_at(now);
    let decoded = decode(OtlpDecodeOptions {
        strategy,
        accumulator: Some(&mut guard),
        additional_resource_attributes: &state.otlp_promote_resource_attributes,
    });
    guard.bound(limits);
    Ok(decoded?)
}

/// Decodes and appends an OTLP gRPC export. Returns the decoded series count on
/// success, which is the ingest `items` measure.
///
/// The router mounts the gRPC service with `route_service`, so the
/// authentication layer has put the request's [`Principal`] into its
/// extensions. A request without one fails closed.
pub(crate) async fn otlp_grpc_export_inner(
    state: &DistributorState,
    request: TonicRequest<ExportMetricsServiceRequest>,
) -> Result<u64, PushError> {
    let principal = request
        .extensions()
        .get::<Principal>()
        .cloned()
        .ok_or(PushError::MissingPrincipal)?;
    let tenant = tenant_from_metadata(request.metadata())?;
    authorize_tenant(&principal, &tenant)?;
    let data = MetricsData {
        resource_metrics: request.into_inner().resource_metrics,
    };
    let mut series =
        decode_tenant_otlp(state, &tenant, TranslationStrategy::default(), |options| {
            decode_otlp_inner(&data, options)
        })?;
    let items = series.len() as u64;
    if append_decoded_series(state, &tenant, &mut series).await?
        && let Some(metrics) = &state.metrics
    {
        metrics.record_ingest_series(tenant.as_str(), items);
    }
    Ok(items)
}

/// Builds a tonic server for OTLP metrics export.
#[must_use]
pub fn otlp_metrics_service_server(
    state: Arc<DistributorState>,
) -> MetricsServiceServer<OtlpMetricsService> {
    let max_decompressed = state.max_decompressed.bytes_usize();
    MetricsServiceServer::new(otlp_metrics_service(state))
        .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
        .max_decoding_message_size(max_decompressed)
}

pub(crate) fn otlp_translation_strategy(
    headers: &HeaderMap,
) -> Result<TranslationStrategy, OtlpError> {
    if let Some(strategy) = header(headers, "x-mimir-otlp-translationstrategy")? {
        return TranslationStrategy::from_str(strategy);
    }

    let Some(add_suffixes) = header(headers, "x-mimir-otlp-addsuffixes")? else {
        return Ok(TranslationStrategy::default());
    };
    let add_suffixes = parse_bool(add_suffixes).ok_or_else(|| {
        OtlpError::Invalid(
            "X-Mimir-OTLP-AddSuffixes".into(),
            format!("invalid boolean `{add_suffixes}`"),
        )
    })?;
    Ok(if add_suffixes {
        TranslationStrategy::UnderscoreEscapingWithSuffixes
    } else {
        TranslationStrategy::UnderscoreEscapingWithoutSuffixes
    })
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Result<Option<&'a str>, OtlpError> {
    headers
        .get(name)
        .map(|value| {
            value.to_str().map_err(|_| {
                OtlpError::Invalid(name.into(), "the header is not valid UTF-8".into())
            })
        })
        .transpose()
}

fn parse_bool(value: &str) -> Option<bool> {
    match value {
        "1" | "t" | "T" | "true" | "TRUE" | "True" => Some(true),
        "0" | "f" | "F" | "false" | "FALSE" | "False" => Some(false),
        _ => None,
    }
}
