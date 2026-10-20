use super::{
    ApiError, MetadataParams, MetadataRecord, MetricStore, PrometheusApiState, Rejection,
    RequestAuth, apply_limit, parse_metadata_params,
};

pub(crate) struct LimitedMetadata {
    pub(crate) params: MetadataParams,
    pub(crate) records: Vec<MetadataRecord>,
}

/// Reads the tenant's metric metadata a metadata request asks for, truncated
/// to its `limit`, with the parsed request parameters. An `Err` is the error
/// response that ends the request.
pub(crate) async fn limited_metadata<S: MetricStore>(
    state: &PrometheusApiState<S>,
    auth: RequestAuth<'_>,
    raw_query: Option<&str>,
) -> Result<LimitedMetadata, Rejection> {
    let metadata_params = parse_metadata_params(raw_query).map_err(Rejection::of)?;
    let tenant = auth.tenant()?;
    let scan = state
        .store
        .metadata(tenant.as_str(), metadata_params.metric.as_deref())
        .await
        .map_err(|error| Rejection::of(ApiError::from(error)))?;
    let mut metadata = scan.metadata;
    apply_limit(&mut metadata, metadata_params.limit);
    Ok(LimitedMetadata {
        params: metadata_params,
        records: metadata,
    })
}
