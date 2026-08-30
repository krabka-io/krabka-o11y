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
) -> BTreeMap<String, String> {
    map.iter()
        .map(|(name, text)| {
            let expanded = crate::http_api::expand_alert_template(text, value, series_labels);
            (name.clone(), expanded)
        })
        .collect()
}
