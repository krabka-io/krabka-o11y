use serde_json::Value;

use super::{BTreeMap, Labels};

/// Expands `{{ $value }}` and `{{ $labels.NAME }}` in every value of an alert
/// label or annotation map.
///
/// This function resolves `$labels` against the series labels of the firing
/// sample.
pub(crate) fn expand_alert_label_map(
    map: &BTreeMap<String, String>,
    value: f64,
    series_labels: &Labels,
    external_labels: &Labels,
    external_url: &str,
    queries: &BTreeMap<String, Value>,
) -> BTreeMap<String, String> {
    map.iter()
        .map(|(name, text)| {
            let expanded = crate::http_api::expand_alert_template_with_queries(
                text,
                value,
                series_labels,
                external_labels,
                external_url,
                queries,
            );
            (name.clone(), expanded)
        })
        .collect()
}
