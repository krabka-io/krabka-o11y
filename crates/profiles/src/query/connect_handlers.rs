use super::{
    Arc, ConnectError, ConnectQueryRequest, ConnectRequest, ConnectResponse, Extension, HeaderMap,
    Principal, ProfileStore, QuerierState, analyze_query_inner, diff_inner,
    get_profile_stats_inner, label_names_inner, label_values_inner, pb, profile_types_inner,
    select_heatmap_inner, select_merge_profile_inner, select_merge_span_profile_inner,
    select_merge_stacktraces_inner, select_series_inner, series_inner, timed_connect_query,
    timed_query,
};

pub(crate) async fn analyze_query_handler<S>(
    state: Extension<Arc<QuerierState<S>>>,
    principal: Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::AnalyzeQueryRequest>,
) -> Result<ConnectResponse<pb::querier::v1::AnalyzeQueryResponse>, ConnectError>
where
    S: ProfileStore,
{
    let metrics = state.0.metrics.clone();
    timed_query(
        &metrics,
        "analyze_query",
        analyze_query_inner(ConnectQueryRequest::new(state, principal, headers, req)),
    )
    .await
}

pub(crate) async fn diff_handler<S>(
    state: Extension<Arc<QuerierState<S>>>,
    principal: Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::DiffRequest>,
) -> Result<ConnectResponse<pb::querier::v1::DiffResponse>, ConnectError>
where
    S: ProfileStore,
{
    let metrics = state.0.metrics.clone();
    timed_query(
        &metrics,
        "diff",
        diff_inner(ConnectQueryRequest::new(state, principal, headers, req)),
    )
    .await
}

pub(crate) async fn get_profile_stats_handler<S>(
    state: Extension<Arc<QuerierState<S>>>,
    principal: Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::GetProfileStatsRequest>,
) -> Result<ConnectResponse<pb::querier::v1::GetProfileStatsResponse>, ConnectError>
where
    S: ProfileStore,
{
    let metrics = state.0.metrics.clone();
    timed_query(
        &metrics,
        "profile_stats",
        get_profile_stats_inner(state, principal, headers, req),
    )
    .await
}

pub(crate) async fn select_heatmap_handler<S>(
    state: Extension<Arc<QuerierState<S>>>,
    principal: Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::SelectHeatmapRequest>,
) -> Result<ConnectResponse<pb::querier::v1::SelectHeatmapResponse>, ConnectError>
where
    S: ProfileStore,
{
    let metrics = state.0.metrics.clone();
    timed_query(
        &metrics,
        "select_heatmap",
        select_heatmap_inner(ConnectQueryRequest::new(state, principal, headers, req)),
    )
    .await
}

pub(crate) async fn select_merge_profile_handler<S>(
    state: Extension<Arc<QuerierState<S>>>,
    principal: Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::SelectMergeProfileRequest>,
) -> Result<ConnectResponse<pb::google::v1::Profile>, ConnectError>
where
    S: ProfileStore,
{
    let metrics = state.0.metrics.clone();
    timed_query(
        &metrics,
        "select_merge_profile",
        select_merge_profile_inner(ConnectQueryRequest::new(state, principal, headers, req)),
    )
    .await
}

pub(crate) async fn select_merge_span_profile_handler<S>(
    state: Extension<Arc<QuerierState<S>>>,
    principal: Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::SelectMergeSpanProfileRequest>,
) -> Result<ConnectResponse<pb::querier::v1::SelectMergeSpanProfileResponse>, ConnectError>
where
    S: ProfileStore,
{
    let metrics = state.0.metrics.clone();
    timed_query(
        &metrics,
        "select_merge_span_profile",
        select_merge_span_profile_inner(ConnectQueryRequest::new(state, principal, headers, req)),
    )
    .await
}

pub(crate) async fn select_merge_stacktraces_handler<S>(
    state: Extension<Arc<QuerierState<S>>>,
    principal: Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::SelectMergeStacktracesRequest>,
) -> Result<ConnectResponse<pb::querier::v1::SelectMergeStacktracesResponse>, ConnectError>
where
    S: ProfileStore + 'static,
{
    let metrics = state.0.metrics.clone();
    timed_query(
        &metrics,
        "select_merge_stacktraces",
        select_merge_stacktraces_inner(ConnectQueryRequest::new(state, principal, headers, req)),
    )
    .await
}

pub(crate) async fn select_series_handler<S>(
    state: Extension<Arc<QuerierState<S>>>,
    principal: Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::SelectSeriesRequest>,
) -> Result<ConnectResponse<pb::querier::v1::SelectSeriesResponse>, ConnectError>
where
    S: ProfileStore,
{
    timed_connect_query(
        state,
        principal,
        headers,
        req,
        "select_series",
        select_series_inner,
    )
    .await
}

pub(crate) async fn profile_types_handler<S>(
    state: Extension<Arc<QuerierState<S>>>,
    principal: Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::ProfileTypesRequest>,
) -> Result<ConnectResponse<pb::querier::v1::ProfileTypesResponse>, ConnectError>
where
    S: ProfileStore,
{
    let metrics = state.0.metrics.clone();
    timed_query(
        &metrics,
        "profile_types",
        profile_types_inner(ConnectQueryRequest::new(state, principal, headers, req)),
    )
    .await
}

pub(crate) async fn series_handler<S>(
    state: Extension<Arc<QuerierState<S>>>,
    principal: Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::SeriesRequest>,
) -> Result<ConnectResponse<pb::querier::v1::SeriesResponse>, ConnectError>
where
    S: ProfileStore,
{
    timed_connect_query(state, principal, headers, req, "series", series_inner).await
}

pub(crate) async fn label_names_handler<S>(
    state: Extension<Arc<QuerierState<S>>>,
    principal: Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::LabelNamesRequest>,
) -> Result<ConnectResponse<pb::querier::v1::LabelNamesResponse>, ConnectError>
where
    S: ProfileStore,
{
    timed_connect_query(
        state,
        principal,
        headers,
        req,
        "label_names",
        label_names_inner,
    )
    .await
}

pub(crate) async fn label_values_handler<S>(
    state: Extension<Arc<QuerierState<S>>>,
    principal: Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::LabelValuesRequest>,
) -> Result<ConnectResponse<pb::querier::v1::LabelValuesResponse>, ConnectError>
where
    S: ProfileStore,
{
    timed_connect_query(
        state,
        principal,
        headers,
        req,
        "label_values",
        label_values_inner,
    )
    .await
}
