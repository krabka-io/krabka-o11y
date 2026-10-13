use super::{Labels, quote_logql_string};

pub(crate) fn loki_stale_sample_label_set(labels: &Labels) -> String {
    let values = labels
        .iter()
        .map(|entry| format!("{}={}", entry.0, quote_logql_string(entry.1)))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{{{values}}}")
}
