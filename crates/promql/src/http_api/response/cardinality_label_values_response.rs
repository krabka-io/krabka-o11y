use super::{json, Labels, Value, BTreeMap, BTreeSet, SeriesFingerprint, apply_limit};

/// Builds the Grafana Mimir `/cardinality/label_values` response from a series set.
///
/// Shape: `{ "series_count_total": N, "labels": [{ "label_name": ..,
/// "label_values_count": k, "series_count": s, "cardinality": [{
/// "label_value": .., "series_count": c }, ..] }, ..] }`.
///
/// This function sorts `labels` by `series_count` DESC, then by `label_name`
/// ASC. It sorts each nested `cardinality` by `series_count` DESC, then by
/// `label_value` ASC. A `limit` greater than 0 truncates each nested
/// `cardinality` array, as the per-label limit of Mimir does.
pub(crate) fn cardinality_label_values_response(
    series: &[Labels],
    label_names: &[String],
    limit: Option<usize>,
) -> Value {
    // For each (label_name, label_value), the distinct series carrying it.
    let mut series_by_value =
        BTreeMap::<String, BTreeMap<String, BTreeSet<SeriesFingerprint>>>::new();
    let mut total_series = BTreeSet::<SeriesFingerprint>::new();
    for labels in series {
        let fp = labels.fingerprint();
        total_series.insert(fp);
        for (name, value) in labels.iter() {
            if !label_names.is_empty() && !label_names.iter().any(|wanted| wanted == name) {
                continue;
            }
            series_by_value
                .entry(name.clone())
                .or_default()
                .entry(value.clone())
                .or_default()
                .insert(fp);
        }
    }

    let mut labels_out = series_by_value
        .into_iter()
        .map(|(label_name, values)| {
            let label_values_count = values.len();
            let series_count: usize = values.values().flatten().collect::<BTreeSet<_>>().len();
            let mut value_cardinality = values
                .into_iter()
                .map(|(label_value, fingerprints)| (label_value, fingerprints.len()))
                .collect::<Vec<_>>();
            value_cardinality.sort_by(|(left_value, left_count), (right_value, right_count)| {
                right_count
                    .cmp(left_count)
                    .then_with(|| left_value.cmp(right_value))
            });
            apply_limit(&mut value_cardinality, limit);
            (
                label_name,
                label_values_count,
                series_count,
                value_cardinality,
            )
        })
        .collect::<Vec<_>>();
    labels_out.sort_by(
        |(left_name, _, left_series, _), (right_name, _, right_series, _)| {
            right_series
                .cmp(left_series)
                .then_with(|| left_name.cmp(right_name))
        },
    );

    let labels_json = labels_out
        .into_iter()
        .map(
            |(label_name, label_values_count, series_count, value_cardinality)| {
                let cardinality = value_cardinality
                    .into_iter()
                    .map(|(label_value, count)| {
                        json!({
                            "label_value": label_value,
                            "series_count": count,
                        })
                    })
                    .collect::<Vec<_>>();
                json!({
                    "label_name": label_name,
                    "label_values_count": label_values_count,
                    "series_count": series_count,
                    "cardinality": cardinality,
                })
            },
        )
        .collect::<Vec<_>>();

    json!({
        "series_count_total": total_series.len(),
        "labels": labels_json,
    })
}
