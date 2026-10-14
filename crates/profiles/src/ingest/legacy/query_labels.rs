use super::{IngestQuery, Labels};

/// The series labels a legacy upload gets, before the sample-type split.
///
/// `metric_name` is the `__name__` the profile's own shape decides, which is
/// never the `?name=` application: Pyroscope keeps that as `service_name` and
/// names the series after the profile type. The `?spyName=` profiler travels
/// as `pyroscope_spy`. Labels written in the `name=app{k=v}` set, and the
/// `extra_labels` a multipart `labels` part carries, override both.
pub(crate) fn query_labels(
    query: &IngestQuery,
    metric_name: &str,
    extra_labels: Vec<krabka_blockstore::LabelPair>,
) -> Labels {
    let mut labels = Labels::new();
    labels.insert("__name__", metric_name.to_string());
    labels.insert("service_name", query.name.clone());
    labels.insert("pyroscope_spy", query.spy_name.clone());
    for label in &query.labels {
        labels.insert(label.name.clone(), label.value.clone());
    }
    for label in extra_labels {
        labels.insert(label.name, label.value);
    }
    labels
}
