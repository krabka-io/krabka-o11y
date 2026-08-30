use super::{
    BTreeMap, BTreeSet, HeaderMap, HttpQueryError, QuerierState, SeriesParams, TimeRange, Value,
    json, parse_detected_labels_params, series_data, validate_loki_volume_query_range_limit,
    validate_query_length_limit,
};

pub(crate) async fn execute_detected_labels_query(
    state: &QuerierState,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Result<Value, HttpQueryError> {
    let params = parse_detected_labels_params(raw_query)?;
    let time_range = TimeRange::new(params.start, params.end)?;
    validate_loki_volume_query_range_limit(time_range)?;
    if let Some(query) = &params.query {
        validate_query_length_limit(state, query)?;
    }
    let series_params = SeriesParams {
        matchers: params.query.into_iter().collect(),
        start: Some(params.start),
        end: Some(params.end),
        since: None,
    };
    let label_sets = series_data(state, headers, &series_params).await?;
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
