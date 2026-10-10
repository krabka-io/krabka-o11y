use std::fmt::Write as _;

use serde_json::{Value, json};

use super::{Case, CaseResult, InMemorySpanStore, TraceqlEngine};
use crate::result::TraceMetricSeries;

pub(crate) async fn run_metrics_case(
    engine: &TraceqlEngine<InMemorySpanStore>,
    case: Case,
) -> CaseResult {
    let mut result = CaseResult {
        name: case.name,
        passed: false,
        passed_assertions: 0,
        total_assertions: 1 + usize::from(case.expect_metrics.is_some()),
        message: String::new(),
    };
    let Some(query) = case.query else {
        result.message = "missing query".into();
        return result;
    };
    let response = match engine.query_range("t", &query, 0, 10_000, 10_000).await {
        Ok(response) => response,
        Err(err) => {
            result.message = err.to_string();
            return result;
        }
    };
    let expected = case.expect_series_count.unwrap_or(0);
    let actual = response.series.len();
    if actual == expected {
        result.passed_assertions = 1;
    } else {
        result.message = format!("series count expected {expected}, got {actual}");
    }
    if let Some(expected_json) = case.expect_metrics {
        let mut expected: Value = match serde_json::from_str(&expected_json) {
            Ok(expected) => expected,
            Err(err) => {
                result.message = format!("invalid expect_metrics: {err}");
                return result;
            }
        };
        let mut actual = metrics_json(&response.series);
        normalize_metrics(&mut expected);
        normalize_metrics(&mut actual);
        if actual == expected {
            result.passed_assertions += 1;
        } else {
            write!(
                result.message,
                " metrics expected {expected:?}, got {actual:?}"
            )
            .expect("writing to a String cannot fail");
        }
    }
    result.passed = result.passed_assertions == result.total_assertions;
    result
}

fn metric_value(value: f64) -> Value {
    if value.is_nan() {
        json!("NaN")
    } else if value.is_infinite() {
        json!(if value.is_sign_positive() {
            "+Inf"
        } else {
            "-Inf"
        })
    } else {
        json!(value)
    }
}

fn metrics_json(series: &[TraceMetricSeries]) -> Value {
    json!(series.iter().map(|series| {
        let mut result = json!({
            "labels": series.labels,
            "points": series.points.iter().map(|(timestamp, value)| (*timestamp, metric_value(*value))).collect::<Vec<_>>(),
            "exemplars": series.exemplars.iter().map(|exemplar| {
                let mut value = json!({"labels": exemplar.labels, "value": metric_value(exemplar.value), "timestamp_ns": exemplar.timestamp_ns});
                if !exemplar.label_types.is_empty() { value["label_types"] = json!(exemplar.label_types); }
                value
            }).collect::<Vec<_>>(),
        });
        if !series.label_types.is_empty() { result["label_types"] = json!(series.label_types); }
        result
    }).collect::<Vec<_>>())
}

fn normalize_metrics(value: &mut Value) {
    let Some(series) = value.as_array_mut() else {
        return;
    };
    // Series and label maps have no response-order contract. Point order
    // and exemplar contents remain part of the exact comparison.
    for item in series.iter_mut() {
        if let Some(labels) = item.get_mut("labels").and_then(Value::as_array_mut) {
            labels.sort_by_cached_key(Value::to_string);
        }
        if let Some(exemplars) = item.get_mut("exemplars").and_then(Value::as_array_mut) {
            for exemplar in exemplars {
                if let Some(labels) = exemplar.get_mut("labels").and_then(Value::as_array_mut) {
                    labels.sort_by_cached_key(Value::to_string);
                }
            }
        }
    }
    series.sort_by_cached_key(|series| {
        (
            series["labels"].to_string(),
            series["label_types"].to_string(),
        )
    });
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::assert;

    use super::*;
    use crate::result::TraceMetricExemplar;

    fn case(series: &[TraceMetricSeries]) -> Case {
        Case {
            name: "exact metrics".into(),
            kind: "metrics".into(),
            query: Some(r#"{ .svc = "x" } | count_over_time() | by(span.svc)"#.into()),
            expect_series_count: Some(1),
            expect_metrics: Some(metrics_json(series).to_string()),
            ..Case::default()
        }
    }

    #[tokio::test]
    async fn exact_metrics_reject_wrong_values_labels_timestamps_and_exemplars() {
        let mut engine = super::super::engine();
        engine.opts.max_exemplars = 1;
        // The fixture has one svc=x span: trace 2, span 1, start 1001ns.
        let expected = vec![TraceMetricSeries {
            label_types: BTreeMap::default(),
            labels: vec![("span.svc".into(), "x".into())],
            points: vec![(0, 1.0), (10_000, 0.0)],
            exemplars: vec![TraceMetricExemplar {
                label_types: BTreeMap::default(),
                labels: vec![
                    (
                        "trace:id".into(),
                        "02".repeat(16).trim_start_matches('0').into(),
                    ),
                    (".svc".into(), "x".into()),
                    ("span.svc".into(), "x".into()),
                ],
                value: f64::NAN,
                timestamp_ns: 1_001,
            }],
        }];
        let correct = run_metrics_case(&engine, case(&expected)).await;
        assert!(correct.passed, "{}", correct.message);
        assert!(correct.passed_assertions == 2 && correct.total_assertions == 2);

        let mut wrong_value = expected.clone();
        wrong_value[0].points[0].1 = 2.0;
        let mut wrong_label = expected.clone();
        wrong_label[0].labels[0].1 = "a".into();
        let mut wrong_timestamp = expected.clone();
        wrong_timestamp[0].points[0].0 = 1;
        let mut wrong_type = expected.clone();
        wrong_type[0]
            .label_types
            .insert("span.svc".into(), crate::TraceMetricLabelType::Int);
        let mut wrong_exemplar = expected;
        wrong_exemplar[0].exemplars[0].labels[0].1 = "03".repeat(16);
        for wrong in [
            wrong_value,
            wrong_label,
            wrong_timestamp,
            wrong_exemplar,
            wrong_type,
        ] {
            let result = run_metrics_case(&engine, case(&wrong)).await;
            assert!(
                !result.passed,
                "a correct count must not hide incorrect metrics"
            );
            assert!(result.passed_assertions == 1 && result.total_assertions == 2);
            assert!(result.message.contains("metrics expected"));
        }
    }

    #[tokio::test]
    async fn malformed_metrics_expectations_fail_even_when_the_count_matches() {
        let mut malformed = case(&[]);
        malformed.expect_metrics = Some("not JSON".into());
        let result = run_metrics_case(&super::super::engine(), malformed).await;
        assert!(!result.passed);
        assert!(result.passed_assertions == 1 && result.total_assertions == 2);
        assert!(result.message.starts_with("invalid expect_metrics:"));
    }
    #[test]
    fn nonfinite_values_use_explicit_tokens_and_reject_null_or_finite_values() {
        let series = [TraceMetricSeries {
            label_types: BTreeMap::default(),
            labels: Vec::new(),
            points: vec![(0, f64::NAN), (1, f64::INFINITY), (2, f64::NEG_INFINITY)],
            exemplars: vec![TraceMetricExemplar {
                label_types: BTreeMap::default(),
                labels: Vec::new(),
                value: f64::NAN,
                timestamp_ns: 0,
            }],
        }];
        let actual = metrics_json(&series);
        let expected = json!([{
            "labels": [],
            "points": [[0, "NaN"], [1, "+Inf"], [2, "-Inf"]],
            "exemplars": [{"labels": [], "value": "NaN", "timestamp_ns": 0}],
        }]);
        assert!(actual == expected);
        for replacement in [Value::Null, json!(0.0), json!("+Inf")] {
            let mut wrong = expected.clone();
            wrong[0]["points"][0][1] = replacement;
            assert!(actual != wrong);
        }
        let mut wrong = expected;
        wrong[0]["exemplars"][0]["value"] = Value::Null;
        assert!(actual != wrong);
    }
}
