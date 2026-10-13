use super::{
    BTreeMap, ExemplarRecord, Labels, Value, json, labels_json, labels_key, sample_string,
    timestamp_seconds,
};

struct ExemplarGroup {
    series_labels: Labels,
    exemplars: Vec<Value>,
}

pub(crate) fn exemplars_json(exemplars: Vec<ExemplarRecord>) -> Vec<Value> {
    let mut groups = BTreeMap::<String, ExemplarGroup>::new();
    for exemplar in exemplars {
        let key = labels_key(&exemplar.series_labels);
        let labels_json = labels_json(exemplar.labels.iter());
        let value = json!({
            "labels": labels_json,
            "value": sample_string(exemplar.value),
            "timestamp": timestamp_seconds(exemplar.ts_ms),
        });
        groups
            .entry(key)
            .or_insert_with(|| ExemplarGroup {
                series_labels: exemplar.series_labels,
                exemplars: Vec::new(),
            })
            .exemplars
            .push(value);
    }

    groups
        .into_values()
        .map(|group| {
            json!({
                "seriesLabels": labels_json(group.series_labels.iter()),
                "exemplars": group.exemplars,
            })
        })
        .collect()
}
