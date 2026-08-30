use super::*;

pub(crate) fn query_labels(query: &IngestQuery, extra_labels: Vec<(String, String)>) -> Labels {
    let mut labels = Labels::new();
    labels.insert("__name__", query.name.clone());
    for (name, value) in &query.labels {
        labels.insert(name.clone(), value.clone());
    }
    for (name, value) in extra_labels {
        labels.insert(name, value);
    }
    labels
}
