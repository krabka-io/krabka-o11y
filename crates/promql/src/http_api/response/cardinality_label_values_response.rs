use super::{BTreeMap, BTreeSet, Labels, SeriesFingerprint, Value, apply_limit, json};

struct LabelValueCardinality {
    label_value: crate::PromqlString,
    series_count: usize,
}

struct LabelCardinalitySummary {
    label_name: String,
    label_values_count: usize,
    series_count: usize,
    value_cardinality: Vec<LabelValueCardinality>,
}

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
        BTreeMap::<String, BTreeMap<crate::PromqlString, BTreeSet<SeriesFingerprint>>>::new();
    let mut total_series = BTreeSet::<SeriesFingerprint>::new();
    for labels in series {
        let fp = labels.fingerprint();
        total_series.insert(fp);
        for pair in labels.iter() {
            if !label_names.is_empty() && !label_names.iter().any(|wanted| wanted == pair.name) {
                continue;
            }
            series_by_value
                .entry(pair.name.clone())
                .or_default()
                .entry(pair.value.clone())
                .or_default()
                .insert(fp);
        }
    }

    let mut labels_out = Vec::with_capacity(series_by_value.len());
    for entry in series_by_value {
        let label_name = entry.0;
        let values = entry.1;
        let label_values_count = values.len();
        let series_count: usize = values.values().flatten().collect::<BTreeSet<_>>().len();
        let mut value_cardinality = Vec::with_capacity(values.len());
        for value_entry in values {
            value_cardinality.push(LabelValueCardinality {
                label_value: value_entry.0,
                series_count: value_entry.1.len(),
            });
        }
        value_cardinality.sort_by(|left, right| {
            right
                .series_count
                .cmp(&left.series_count)
                .then_with(|| left.label_value.cmp(&right.label_value))
        });
        apply_limit(&mut value_cardinality, limit);
        labels_out.push(LabelCardinalitySummary {
            label_name,
            label_values_count,
            series_count,
            value_cardinality,
        });
    }
    labels_out.sort_by(|left, right| {
        right
            .series_count
            .cmp(&left.series_count)
            .then_with(|| left.label_name.cmp(&right.label_name))
    });

    let mut labels_json = Vec::with_capacity(labels_out.len());
    for summary in labels_out {
        let mut cardinality = Vec::with_capacity(summary.value_cardinality.len());
        for item in summary.value_cardinality {
            cardinality.push(json!({
                "label_value": item.label_value.as_str(),
                "series_count": item.series_count,
            }));
        }
        labels_json.push(json!({
            "label_name": summary.label_name,
            "label_values_count": summary.label_values_count,
            "series_count": summary.series_count,
            "cardinality": cardinality,
        }));
    }

    json!({
        "series_count_total": total_series.len(),
        "labels": labels_json,
    })
}
