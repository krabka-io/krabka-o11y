use super::{
    AnalyticsQuery, HeaderMap, HttpQueryError, QuerierState, RequestSecurity, TenantErrorSurface,
    TimeRange, Value, VolumeKind, VolumeRangeLimit, add_loki_query_stats_for_stream_plan,
    authorized_tenant, index_volume_samples, json, loki_volume_matrix_response,
    loki_volume_vector_response, parse_volume_params, plan_analytics_query,
};

pub(crate) async fn execute_index_volume_query(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    raw_query: Option<&str>,
    kind: VolumeKind,
) -> Result<Value, HttpQueryError> {
    let params = parse_volume_params(raw_query)?;
    let tenant = authorized_tenant(state, security, headers, TenantErrorSurface::Read).await?;
    let time_range = TimeRange::new(params.start, params.end)?;
    let (state, plan, _) = plan_analytics_query(
        state,
        AnalyticsQuery {
            tenant: &tenant,
            time_range,
            query: &params.query,
            volume_range_limit: VolumeRangeLimit::Check,
        },
    )
    .await?;
    let tenant = tenant.as_str();
    let volumes = index_volume_samples(&state, tenant, &plan, &params);
    let mut response = match kind {
        VolumeKind::Instant => loki_volume_vector_response(volumes, params.end, params.limit),
        VolumeKind::Range => {
            if params.step.is_some_and(|step| step <= 0) {
                return Err(HttpQueryError::InvalidStep);
            }
            // Loki writes an empty `volume_range` answer as a `vector`, and
            // one with series in it as a `matrix`.
            if volumes.is_empty() {
                loki_volume_vector_response(volumes, params.end, params.limit)
            } else {
                loki_volume_matrix_response(volumes, params.limit)
            }
        }
    };
    // A volume read scans no lines, and Loki's stats for one say so. The stats
    // are built from an empty answer, so a `matrix`'s samples are not counted
    // as lines processed.
    response["data"]["stats"] =
        add_loki_query_stats_for_stream_plan(json!({}), &plan)["data"]["stats"].take();
    Ok(response)
}
