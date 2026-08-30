use super::*;

pub(crate) fn loki_label_set(labels: &Labels) -> String {
    let values = labels
        .iter()
        .map(|(name, value)| format!("{name}={}", quote_logql_string(value)))
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{values}}}")
}
