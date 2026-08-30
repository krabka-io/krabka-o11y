use super::*;

pub(crate) fn loki_metric_sample([timestamp_ns, value]: [String; 2]) -> Value {
    json!([unix_ns_string_to_loki_seconds(&timestamp_ns), value])
}
