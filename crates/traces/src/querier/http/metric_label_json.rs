use krabka_traceql::TraceMetricLabelType;

use super::{Value, json, metric_value_json};

/// One TraceQL-metrics label as Tempo's protojson `commonv1.KeyValue`, which is
/// `{"key": k, "value": {"stringValue": v}}`.
///
/// Grafana's Tempo backend parses the `labels` field as a JSON array, so a map
/// object fails to unmarshal with
/// `cannot unmarshal object into Go value of type []json.RawMessage`.
pub(crate) fn metric_label_json(
    key: &str,
    value: &str,
    kind: Option<TraceMetricLabelType>,
) -> Value {
    match kind {
        Some(TraceMetricLabelType::Int) => return json!({"key":key,"value":{"intValue":value}}),
        Some(TraceMetricLabelType::Double) => {
            return json!({"key":key,"value":{"doubleValue":metric_value_json(value.parse().expect("double metric label is produced from f64"))}});
        }
        Some(TraceMetricLabelType::Bool) => {
            return json!({"key":key,"value":{"boolValue":value.parse::<bool>().expect("boolean metric label is produced from bool")}});
        }
        None => {}
    }

    if matches!(key, "p" | "__bucket")
        && let Ok(number) = value.parse::<f64>()
    {
        return json!({"key":key,"value":{"doubleValue":number}});
    }
    json!({ "key": key, "value": { "stringValue": value } })
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn scalar_labels_preserve_anyvalue_type_and_nonfinite_doubles() {
        for (text, kind, expected) in [
            ("1", None, json!({"stringValue":"1"})),
            (
                "1",
                Some(TraceMetricLabelType::Int),
                json!({"intValue":"1"}),
            ),
            (
                "1",
                Some(TraceMetricLabelType::Double),
                json!({"doubleValue":1.0}),
            ),
            (
                "false",
                Some(TraceMetricLabelType::Bool),
                json!({"boolValue":false}),
            ),
            (
                "NaN",
                Some(TraceMetricLabelType::Double),
                json!({"doubleValue":"NaN"}),
            ),
            (
                "inf",
                Some(TraceMetricLabelType::Double),
                json!({"doubleValue":"Infinity"}),
            ),
        ] {
            assert!(
                metric_label_json("span.mixed", text, kind)
                    == json!({"key":"span.mixed","value":expected})
            );
        }
    }
}
