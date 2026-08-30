use super::*;

pub(crate) fn exemplars_json(exemplars: Vec<ExemplarRecord>) -> Vec<Value> {
    let mut groups = BTreeMap::<String, (Labels, Vec<Value>)>::new();
    for exemplar in exemplars {
        let key = labels_key(&exemplar.series_labels);
        let labels_json = labels_json(&exemplar.labels);
        let value = json!({
            "labels": labels_json,
            "value": sample_string(exemplar.value),
            "timestamp": timestamp_seconds(exemplar.ts_ms),
        });
        groups
            .entry(key)
            .or_insert_with(|| (exemplar.series_labels, Vec::new()))
            .1
            .push(value);
    }

    groups
        .into_values()
        .map(|(series_labels, exemplars)| {
            json!({
                "seriesLabels": labels_json(&series_labels),
                "exemplars": exemplars,
            })
        })
        .collect()
}
