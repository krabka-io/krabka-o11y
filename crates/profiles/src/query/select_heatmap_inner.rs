use super::{
    Arc, BTreeMap, ConnectError, ConnectRequest, ConnectResponse, EndMs, Extension, HeaderMap,
    ProfileStore, QuerierState, StartMs, connect_error, heatmap_time_buckets, limit, pb,
    step_from_secs, tenant_from_headers,
};

pub(crate) async fn select_heatmap_inner<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::SelectHeatmapRequest>,
) -> Result<ConnectResponse<pb::querier::v1::SelectHeatmapResponse>, ConnectError>
where
    S: ProfileStore,
{
    let tenant = tenant_from_headers(&headers).map_err(connect_error)?;
    let req = req.0;
    state
        .validate_query_range(&tenant, req.start, req.end)
        .map_err(connect_error)?;
    let step = step_from_secs(req.step).map_err(connect_error)?;
    let time_buckets = heatmap_time_buckets(
        StartMs(req.start),
        EndMs(req.end),
        step,
        state.heatmap_time_buckets_max,
    )
    .map_err(connect_error)?;
    let span_exemplars = match req.exemplar_type {
        exemplar_type if exemplar_type == pb::querier::v1::ExemplarType::Span as i32 => state
            .select_heatmap_span_exemplars(
                (&tenant, &req.profile_type_id, &req.label_selector),
                &req.group_by,
                (req.start, req.end),
                time_buckets,
            )
            .await
            .map_err(connect_error)?,
        exemplar_type if exemplar_type == pb::querier::v1::ExemplarType::Individual as i32 => state
            .select_heatmap_individual_exemplars(
                (&tenant, &req.profile_type_id, &req.label_selector),
                &req.group_by,
                (req.start, req.end),
                time_buckets,
            )
            .await
            .map_err(connect_error)?,
        _ => BTreeMap::new(),
    };
    let heatmaps = if req.query_type == pb::querier::v1::HeatmapQueryType::Span as i32 {
        state
            .select_span_heatmaps(
                (&tenant, &req.profile_type_id, &req.label_selector),
                &req.group_by,
                (req.start, req.end),
                time_buckets,
                state.heatmap_value_buckets,
            )
            .await
    } else {
        state
            .engine
            .select_heatmaps(
                (&tenant, &req.profile_type_id, &req.label_selector),
                &req.group_by,
                (req.start, req.end),
                time_buckets,
                state.heatmap_value_buckets,
            )
            .await
    }
    .map_err(connect_error)?;
    let series = heatmaps
        .into_iter()
        .take(limit(req.limit))
        .map(|heatmap| {
            let exemplar_slots = span_exemplars.get(&heatmap.labels);
            let mut series = pb::querier::v1::HeatmapSeries::from(heatmap);
            if let Some(exemplar_slots) = exemplar_slots {
                for slot in &mut series.slots {
                    slot.exemplars = exemplar_slots
                        .get(&slot.timestamp)
                        .cloned()
                        .unwrap_or_default();
                }
            }
            series
        })
        .collect();
    Ok(ConnectResponse::new(
        pb::querier::v1::SelectHeatmapResponse { series },
    ))
}
