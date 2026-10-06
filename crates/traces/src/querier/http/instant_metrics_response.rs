use std::cmp::Ordering;

use serde_json::{Value, json};

use super::metric_value_json;

// Tempo QueryInstantResponse/InstantSeries contains scalar values, with the
// protobuf defaults omitted by jsonpb.Marshaler. One unrestricted execution
// is one completed job; no sample timestamps or exemplars are exposed.
pub(crate) fn instant_metrics_response(series: impl IntoIterator<Item = (Value, f64)>) -> Value {
    let series = series
        .into_iter()
        .map(|(labels, value)| {
            let mut series = serde_json::Map::new();
            if labels.as_array().is_some_and(|labels| !labels.is_empty()) {
                series.insert("labels".into(), labels);
            }
            if value.partial_cmp(&0.0) != Some(Ordering::Equal) {
                series.insert("value".into(), metric_value_json(value));
            }
            Value::Object(series)
        })
        .collect::<Vec<_>>();
    let mut body = json!({"metrics": {"completedJobs": 1, "totalJobs": 1}});
    if !series.is_empty() {
        body["series"] = Value::Array(series);
    }
    body
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn scalar_protojson_preserves_labels_nonfinite_values_and_default_omission() {
        let labels = json!([{"key": "group", "value": {"intValue": "7"}}]);
        assert!(
            instant_metrics_response([(labels.clone(), 3.0)])
                == json!({
                    "series": [{"labels": labels, "value": 3.0}],
                    "metrics": {"completedJobs": 1, "totalJobs": 1}
                })
        );
        for (value, token) in [
            (f64::NAN, "NaN"),
            (f64::INFINITY, "Infinity"),
            (f64::NEG_INFINITY, "-Infinity"),
        ] {
            assert!(
                instant_metrics_response([(json!([]), value)])
                    == json!({
                        "series": [{"value": token}], "metrics": {"completedJobs": 1, "totalJobs": 1}
                    })
            );
        }
        for value in [0.0, -0.0] {
            assert!(
                instant_metrics_response([(json!([]), value)])
                    == json!({
                        "series": [{}], "metrics": {"completedJobs": 1, "totalJobs": 1}
                    })
            );
        }
        assert!(
            instant_metrics_response([])
                == json!({"metrics": {"completedJobs": 1, "totalJobs": 1}})
        );
    }
}
