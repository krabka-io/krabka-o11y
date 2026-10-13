use super::{
    BTreeMap, ConnectError, ConnectRequest, ConnectResponse, ProfileStore, QuerierRequestParts,
    SeriesAgg, SeriesExemplarQuery, connect_error, label_pairs, limit, pb, stack_trace_call_sites,
    step_from_secs,
};

pub(crate) async fn select_series_inner<S>(
    QuerierRequestParts {
        state,
        principal,
        headers,
    }: QuerierRequestParts<S>,
    req: ConnectRequest<pb::querier::v1::SelectSeriesRequest>,
) -> Result<ConnectResponse<pb::querier::v1::SelectSeriesResponse>, ConnectError>
where
    S: ProfileStore,
{
    let tenant = super::authorized_connect_tenant(&headers, &principal, &state.tenant_policy)?;
    let req = req.0;
    let agg = if state.query_architecture == super::PyroscopeQueryArchitecture::V1
        && req.aggregation == Some(pb::querier::v1::SeriesAggregationType::Average as i32)
    {
        SeriesAgg::Average
    } else {
        SeriesAgg::Sum
    };
    let stack_trace_call_sites =
        if state.query_architecture == super::PyroscopeQueryArchitecture::V2 {
            Vec::new()
        } else {
            stack_trace_call_sites(req.stack_trace_selector.as_ref())
        };
    // Pyroscope carries `step` as a float number of seconds on the request; it
    // becomes a `Time` here, at the Connect boundary, so nothing downstream has
    // to remember the unit.
    let step = step_from_secs(req.step).map_err(connect_error)?;
    let span_exemplars = match req.exemplar_type {
        exemplar_type
            if exemplar_type == pb::querier::v1::ExemplarType::Span as i32
                && state.query_architecture == super::PyroscopeQueryArchitecture::V1 =>
        {
            state
                .select_series_span_exemplars(SeriesExemplarQuery {
                    target: super::QueryTarget::new(
                        &tenant,
                        &req.profile_type_id,
                        &req.label_selector,
                    ),
                    group_by: &req.group_by,
                    step,
                    range: super::QueryRange::new(req.start, req.end),
                    call_sites: &stack_trace_call_sites,
                })
                .await
                .map_err(connect_error)?
        }
        exemplar_type if exemplar_type == pb::querier::v1::ExemplarType::Individual as i32 => state
            .select_series_individual_exemplars(SeriesExemplarQuery {
                target: super::QueryTarget::new(&tenant, &req.profile_type_id, &req.label_selector),
                group_by: &req.group_by,
                step,
                range: super::QueryRange::new(req.start, req.end),
                call_sites: &stack_trace_call_sites,
            })
            .await
            .map_err(connect_error)?,
        _ => BTreeMap::new(),
    };
    let mut series = state
        .select_series(
            super::QueryTarget::new(&tenant, &req.profile_type_id, &req.label_selector),
            &req.group_by,
            step,
            agg,
            super::QueryRange::new(req.start, req.end),
            &stack_trace_call_sites,
        )
        .await
        .map_err(connect_error)?;
    // No matching data returns an empty report without invoking the v2
    // exemplar backend. Preserve its populated SPAN failure after planning.
    if state.query_architecture == super::PyroscopeQueryArchitecture::V2
        && req.exemplar_type == pb::querier::v1::ExemplarType::Span as i32
        && !series.is_empty()
    {
        return Err(super::ConnectError::new(
            super::Code::Unknown,
            "span exemplars are unavailable in the v2 series backend",
        ));
    }
    series.sort_by(|left, right| {
        let total = |series: &krabka_pprof::Series| {
            series.points.iter().map(|point| point.value).sum::<f64>()
        };
        total(right)
            .total_cmp(&total(left))
            .then_with(|| left.labels.cmp(&right.labels))
    });
    let series = series
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
                    .map(|point| pb::querier::v1::Point {
                        timestamp: point.timestamp_ms,
                        value: point.value,
                        annotations: Vec::new(),
                        exemplars: exemplar_points
                            .and_then(|points| points.get(&point.timestamp_ms))
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
