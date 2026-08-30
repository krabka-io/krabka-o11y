use super::{json, Labels, Value, labels_json};

/// Builds the Grafana Mimir `/cardinality/active_series` response.
///
/// The response is a bare object with one `data` array of flat label maps. It
/// has no `status` envelope and no `seriesLabels` or `metric` wrapper.
pub(crate) fn active_series_response(series: Vec<Labels>) -> Value {
    let data = series
        .into_iter()
        .map(|labels| labels_json(&labels))
        .collect::<Vec<_>>();
    json!({ "data": data })
}
