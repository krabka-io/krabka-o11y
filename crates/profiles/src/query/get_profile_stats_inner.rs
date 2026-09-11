use super::{
    Arc, ConnectError, ConnectRequest, ConnectResponse, Extension, HeaderMap, Principal,
    ProfileStore, QuerierState, authorize_tenant, connect_error, pb, tenant_connect_error,
    tenant_denied_connect_error, tenant_from_headers,
};

pub(crate) async fn get_profile_stats_inner<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    _req: ConnectRequest<pb::querier::v1::GetProfileStatsRequest>,
) -> Result<ConnectResponse<pb::querier::v1::GetProfileStatsResponse>, ConnectError>
where
    S: ProfileStore,
{
    let tenant = tenant_from_headers(&headers, &state.tenant_policy)
        .map_err(|error| tenant_connect_error(&error))?;
    authorize_tenant(&principal, &tenant).map_err(|denied| tenant_denied_connect_error(&denied))?;
    // GetProfileStats is a global "has this tenant ever ingested, and over what
    // span" query. Pyroscope's request carries no time range, and Grafana's
    // Profiles Drilldown sends an empty message (so start/end arrive as 0).
    // Report stats across all data rather than time-scoping to [0, 0] — the
    // latter always looks empty and wedges the Drilldown onto its onboarding
    // screen even when the tenant has data. No range validation: a global
    // metadata query is unbounded by design (Pyroscope doesn't limit it).
    let profile_stats = state
        .global_profile_stats(&tenant)
        .await
        .map_err(connect_error)?;
    Ok(ConnectResponse::new(
        pb::querier::v1::GetProfileStatsResponse {
            data_ingested: profile_stats.data_ingested,
            oldest_profile_time: profile_stats.oldest_profile_time.unwrap_or_default(),
            newest_profile_time: profile_stats.newest_profile_time.unwrap_or_default(),
        },
    ))
}
