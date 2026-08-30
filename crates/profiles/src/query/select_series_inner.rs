use super::{
    Arc, BTreeMap, ConnectError, ConnectRequest, ConnectResponse, Extension, HeaderMap,
    ProfileStore, QuerierState, SeriesAgg, connect_error, label_pairs, limit, pb,
    stack_trace_call_sites, step_from_secs, tenant_from_headers,
};

pub(crate) async fn select_series_inner<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::SelectSeriesRequest>,
) -> Result<ConnectResponse<pb::querier::v1::SelectSeriesResponse>, ConnectError>
where
    S: ProfileStore,
{
    let tenant = tenant_from_headers(&headers).map_err(connect_error)?;
    let req = req.0;
    let agg = if req.aggregation
        == pb::querier::v1::SeriesAggregationType::TimeSeriesAggregationTypeAverage as i32
    {
        SeriesAgg::Average
    } else {
        SeriesAgg::Sum
    };
    let stack_trace_call_sites = stack_trace_call_sites(req.stack_trace_selector.as_ref());
    // Pyroscope carries `step` as a float number of seconds on the request; it
    // becomes a `Time` here, at the Connect boundary, so nothing downstream has
    // to remember the unit.
    let step = step_from_secs(req.step).map_err(connect_error)?;
    let span_exemplars = match req.exemplar_type {
        exemplar_type if exemplar_type == pb::querier::v1::ExemplarType::Span as i32 => state
            .select_series_span_exemplars(
                (&tenant, &req.profile_type_id, &req.label_selector),
                &req.group_by,
                step,
                (req.start, req.end),
                &stack_trace_call_sites,
            )
            .await
            .map_err(connect_error)?,
        exemplar_type if exemplar_type == pb::querier::v1::ExemplarType::Individual as i32 => state
            .select_series_individual_exemplars(
                (&tenant, &req.profile_type_id, &req.label_selector),
                &req.group_by,
                step,
                (req.start, req.end),
                &stack_trace_call_sites,
            )
            .await
            .map_err(connect_error)?,
        _ => BTreeMap::new(),
    };
    let series = state
        .select_series(
            (&tenant, &req.profile_type_id, &req.label_selector),
            &req.group_by,
            step,
            agg,
            (req.start, req.end),
            &stack_trace_call_sites,
        )
        .await
        .map_err(connect_error)?
        .into_iter()
        .take(limit(req.limit))
        .map(|series| {
            let exemplar_points = span_exemplars.get(&series.labels);
            let labels = label_pairs(series.labels);
            pb::querier::v1::ProfileSeries {
                labels,
                points: series
                    .points
                    .into_iter()
                    .map(|(timestamp, value)| pb::querier::v1::Point {
                        timestamp,
                        value,
                        annotations: Vec::new(),
                        exemplars: exemplar_points
                            .and_then(|points| points.get(&timestamp))
                            .cloned()
                            .unwrap_or_default(),
                    })
                    .collect(),
            }
        })
        .collect();
    Ok(ConnectResponse::new(
        pb::querier::v1::SelectSeriesResponse { series },
    ))
}
