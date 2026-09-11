use super::{
    BTreeMap, BTreeSet, HeaderMap, HttpQueryError, QuerierState, RequestSecurity, SeriesParams,
    TenantErrorSurface, TimeRange, Value, authorized_tenant, json, parse_detected_labels_params,
    series_data, validate_loki_volume_query_range_limit, validate_query_string_bytes_limit,
};

pub(crate) async fn execute_detected_labels_query(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Result<Value, HttpQueryError> {
    let params = parse_detected_labels_params(raw_query)?;
    let tenant = authorized_tenant(state, security, headers, TenantErrorSurface::Read).await?;
    let state = &state.with_tenant_limits(&tenant);
    let time_range = TimeRange::new(params.start, params.end)?;
    validate_loki_volume_query_range_limit(state, time_range)?;
    if let Some(query) = &params.query {
        validate_query_string_bytes_limit(state, query)?;
    }
    let series_params = SeriesParams {
        matchers: params.query.into_iter().collect(),
        start: Some(params.start),
        end: Some(params.end),
        since: None,
    };
    let label_sets = series_data(state, security, headers, &series_params).await?;
    let mut values_by_label = BTreeMap::<String, BTreeSet<String>>::new();
    for labels in label_sets {
        for (name, value) in labels {
            values_by_label.entry(name).or_default().insert(value);
        }
    }
    if values_by_label.is_empty() {
        return Ok(json!({}));
    }

    let detected_labels = values_by_label
        .into_iter()
        .take(params.limit)
        .map(|(label, values)| {
            json!({
                "label": label,
                "cardinality": values.len(),
            })
        })
        .collect::<Vec<_>>();

    Ok(json!({
        "detectedLabels": detected_labels,
    }))
}
