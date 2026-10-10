use super::{
    Arc, BTreeMap, ConnectError, ConnectRequest, ConnectResponse, EndMs, Extension, HeaderMap,
    HeatmapSlotsMillis, Principal, ProfileStore, QuerierState, SpanHeatmapRequest, StartMs,
    TimeExt, authorize_tenant, connect_error, heatmap_from_points, heatmap_time_buckets, limit, pb,
    step_from_secs, tenant_connect_error, tenant_denied_connect_error, tenant_from_headers,
};

pub(crate) async fn select_heatmap_inner<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::SelectHeatmapRequest>,
) -> Result<ConnectResponse<pb::querier::v1::SelectHeatmapResponse>, ConnectError>
where
    S: ProfileStore,
{
    let tenant = tenant_from_headers(&headers, &state.tenant_policy)
        .map_err(|error| tenant_connect_error(&error))?;
    authorize_tenant(&principal, &tenant).map_err(|denied| tenant_denied_connect_error(&denied))?;
    let req = req.0;
    state
        .validate_query_range(&tenant, req.start, req.end)
        .map_err(connect_error)?;
    if req.limit.is_some_and(|limit| limit < 0) {
        return Err(connect_error(super::ProfileError::Plan(
            "limit must be non-negative".to_string(),
        )));
    }
    let step = step_from_secs(req.step).map_err(connect_error)?;
    heatmap_time_buckets(
        StartMs(req.start),
        EndMs(req.end),
        step,
        state.heatmap_time_buckets_max,
    )
    .map_err(connect_error)?;
    let step_ms = step.millis_i64();
    let scan_start = req.start.saturating_sub(step_ms);
    let span_exemplars = match req.exemplar_type {
        exemplar_type if exemplar_type == pb::querier::v1::ExemplarType::Span as i32 => state
            .select_heatmap_span_exemplars(
                (&tenant, &req.profile_type_id, &req.label_selector),
                &req.group_by,
                HeatmapSlotsMillis {
                    start: scan_start,
                    end: req.end,
                    step: step_ms,
                },
            )
            .await
            .map_err(connect_error)?,
        exemplar_type if exemplar_type == pb::querier::v1::ExemplarType::Individual as i32 => state
            .select_heatmap_individual_exemplars(
                (&tenant, &req.profile_type_id, &req.label_selector),
                &req.group_by,
                HeatmapSlotsMillis {
                    start: scan_start,
                    end: req.end,
                    step: step_ms,
                },
            )
            .await
            .map_err(connect_error)?,
        _ => BTreeMap::new(),
    };
    let heatmaps = if req.query_type == pb::querier::v1::HeatmapQueryType::Span as i32 {
        state
            .select_span_heatmap_points(SpanHeatmapRequest {
                tenant: &tenant,
                profile_type: &req.profile_type_id,
                label_selector: &req.label_selector,
                group_by: &req.group_by,
                range: krabka_pprof::MillisRange {
                    start_ms: scan_start,
                    end_ms: req.end,
                },
            })
            .await
    } else {
        state
            .engine
            .select_heatmap_points(
                krabka_pprof::ProfileSelection {
                    tenant: tenant.as_str(),
                    profile_type: &req.profile_type_id,
                    label_selector: &req.label_selector,
                },
                &req.group_by,
                krabka_pprof::MillisRange {
                    start_ms: scan_start,
                    end_ms: req.end,
                },
            )
            .await
    }
    .map_err(connect_error)?;
    if heatmaps.iter().all(|(_, points)| points.is_empty()) {
        return Ok(ConnectResponse::new(
            pb::querier::v1::SelectHeatmapResponse::default(),
        ));
    }
    if state.query_architecture == super::PyroscopeQueryArchitecture::V2
        && (!matches!(req.query_type, 1 | 2)
            || (req.exemplar_type == 2 && req.query_type != 1)
            || (req.exemplar_type == 3 && req.query_type != 2))
    {
        return Err(ConnectError::new(
            super::Code::Unknown,
            "invalid heatmap query and exemplar type combination",
        ));
    }
    let mut series = heatmap_from_points(
        heatmaps,
        req.start,
        req.end,
        step_ms,
        state.heatmap_value_buckets,
    );
    series.sort_by_key(|series| {
        std::cmp::Reverse(
            series
                .slots
                .iter()
                .flat_map(|slot| &slot.counts)
                .map(|count| i64::from(*count))
                .sum::<i64>(),
        )
    });
    let series = series
        .into_iter()
        .take(limit(req.limit))
        .map(|mut series| {
            let labels = series
                .labels
                .iter()
                .map(|label| (label.name.clone(), label.value.clone()))
                .collect::<Vec<_>>();
            let exemplar_slots = span_exemplars.get(&labels);
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
