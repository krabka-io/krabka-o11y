use std::cmp::Ordering;

use assert2::assert;
use serde_json::{Map, Value, json};

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SeedPoint {
    pub metric: &'static str,
    pub labels: &'static [(&'static str, &'static str)],
    pub samples: &'static [(i64, f64)],
}

#[must_use]
pub fn seed_dataset() -> Vec<SeedPoint> {
    vec![
        SeedPoint {
            metric: "up",
            labels: &[("job", "api"), ("instance", "a")],
            samples: &[(0, 1.0), (15_000, 1.0), (30_000, 1.0), (45_000, 1.0)],
        },
        SeedPoint {
            metric: "http_requests_total",
            labels: &[("job", "api"), ("method", "GET"), ("code", "200")],
            samples: &[(0, 0.0), (15_000, 30.0), (30_000, 75.0), (45_000, 120.0)],
        },
        SeedPoint {
            metric: "http_requests_total",
            labels: &[("job", "api"), ("method", "POST"), ("code", "500")],
            samples: &[(0, 0.0), (15_000, 3.0), (30_000, 5.0), (45_000, 8.0)],
        },
        SeedPoint {
            metric: "cpu_temperature_celsius",
            labels: &[("job", "node"), ("instance", "a")],
            samples: &[(0, 40.0), (15_000, 42.0), (30_000, 41.5), (45_000, 43.0)],
        },
        SeedPoint {
            metric: "http_request_duration_seconds_bucket",
            labels: &[("job", "api"), ("le", "0.5")],
            samples: &[(0, 0.0), (15_000, 10.0), (30_000, 25.0), (45_000, 40.0)],
        },
        SeedPoint {
            metric: "http_request_duration_seconds_bucket",
            labels: &[("job", "api"), ("le", "1")],
            samples: &[(0, 0.0), (15_000, 20.0), (30_000, 45.0), (45_000, 70.0)],
        },
        SeedPoint {
            metric: "http_request_duration_seconds_bucket",
            labels: &[("job", "api"), ("le", "+Inf")],
            samples: &[(0, 0.0), (15_000, 25.0), (30_000, 55.0), (45_000, 90.0)],
        },
        SeedPoint {
            metric: "http_request_duration_seconds_sum",
            labels: &[("job", "api")],
            samples: &[(0, 0.0), (15_000, 12.0), (30_000, 30.0), (45_000, 60.0)],
        },
        SeedPoint {
            metric: "http_request_duration_seconds_count",
            labels: &[("job", "api")],
            samples: &[(0, 0.0), (15_000, 25.0), (30_000, 55.0), (45_000, 90.0)],
        },
        SeedPoint {
            metric: "native_histogram_marker",
            labels: &[("job", "api")],
            samples: &[(0, 1.0), (15_000, 1.0), (30_000, 1.0), (45_000, 1.0)],
        },
    ]
}

#[must_use]
pub fn normalize(response: &Value) -> Value {
    normalize_value(response)
}

pub fn assert_query_equal(name: &str, left: &Value, right: &Value) {
    let left = normalize(left);
    let right = normalize(right);
    assert!(
        queries_equal(&left, &right),
        "query `{name}` differed\nleft: {}\nright: {}",
        pretty(&left),
        pretty(&right)
    );
}

/// Compares float sample strings with absolute tolerance 1e-6. Labels, sample
/// timestamps, string results, annotations, nonfinite values, and result shapes
/// remain exact. Pairwise comparison avoids false failures at rounding boundaries.
#[must_use]
pub fn queries_equal(left: &Value, right: &Value) -> bool {
    equivalent_value(&normalize(left), &normalize(right), false, false)
}

fn equivalent_value(left: &Value, right: &Value, numeric: bool, string_result: bool) -> bool {
    match (left, right) {
        (Value::Object(left), Value::Object(right)) => {
            let string_result = string_result || left.get("resultType") == Some(&json!("string"));
            let scalar_result = left.get("resultType") == Some(&json!("scalar"));
            left.len() == right.len()
                && left.iter().all(|(key, left)| {
                    right.get(key).is_some_and(|right| {
                        if key == "metric" {
                            return left == right;
                        }
                        if key == "result" && scalar_result {
                            let (Some(left), Some(right)) = (left.as_array(), right.as_array())
                            else {
                                return false;
                            };
                            return left.len() == 2
                                && right.len() == 2
                                && left[0].is_number()
                                && left[0] == right[0]
                                && left[1].is_string()
                                && right[1].is_string()
                                && equivalent_value(&left[1], &right[1], true, false);
                        }
                        let numeric = !string_result
                            && (numeric
                                || matches!(
                                    key.as_str(),
                                    "value" | "values" | "histogram" | "histograms"
                                ));
                        equivalent_value(left, right, numeric, string_result)
                    })
                })
        }
        (Value::Array(left), Value::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| equivalent_value(left, right, numeric, string_result))
        }
        (Value::String(left), Value::String(right)) if numeric => {
            if left == right {
                return true;
            }
            match (left.parse::<f64>(), right.parse::<f64>()) {
                (Ok(left), Ok(right)) if left.is_finite() && right.is_finite() => {
                    (left - right).abs() <= 1e-6
                }
                _ => false,
            }
        }
        _ => left == right,
    }
}

fn normalize_value(value: &Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.iter().map(normalize_value).collect()),
        Value::Object(object) => normalize_object(object),
        other => other.clone(),
    }
}

fn normalize_object(object: &Map<String, Value>) -> Value {
    let mut out = Map::new();
    for (key, value) in object {
        if is_volatile_field(key) {
            continue;
        }
        out.insert(
            key.clone(),
            if key == "metric" {
                value.clone()
            } else {
                normalize_value(value)
            },
        );
    }

    if let Some(Value::Array(result)) = out.get_mut("result") {
        result.sort_by(compare_series_result);
    }
    for field in ["infos", "warnings"] {
        if let Some(Value::Array(annotations)) = out.get_mut(field) {
            annotations.sort_by_key(Value::to_string);
        }
    }

    Value::Object(out)
}

fn is_volatile_field(key: &str) -> bool {
    key == "stats"
}

fn compare_series_result(left: &Value, right: &Value) -> Ordering {
    series_sort_key(left).cmp(&series_sort_key(right))
}

fn series_sort_key(value: &Value) -> String {
    let labels = value
        .get("metric")
        .filter(|metric| metric.is_object())
        .map(Value::to_string)
        .unwrap_or_default();
    let sample = value
        .get("value")
        .or_else(|| value.get("values"))
        .map_or_else(String::new, Value::to_string);
    format!("{labels}|{sample}")
}

fn pretty(value: &Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
}

#[allow(dead_code)]
fn _native_histogram_shape_marker() -> Value {
    json!({
        "schema": 0,
        "count": "1",
        "sum": "1",
        "zeroCount": "0",
        "positiveSpans": [],
        "negativeSpans": []
    })
}

#[cfg(test)]
mod normalization_controls {
    use super::{assert, json, queries_equal};

    #[test]
    fn numeric_tolerance_preserves_labels_and_large_finite_values() {
        let sample = |label: &str, value: &str| json!({"data":{"result":[{"metric":{"instance":label},"value":[1,value]}]}});
        assert!(queries_equal(&sample("1", "1.00000001"), &sample("1", "1")));
        assert!(!queries_equal(&sample("1", "1"), &sample("1.0", "1")));
        assert!(!queries_equal(&sample("1", "1e308"), &sample("1", "+Inf")));
        assert!(!queries_equal(
            &sample("1", "1e308"),
            &sample("1", "1.1e308")
        ));
        assert!(!queries_equal(&sample("1", "-1e308"), &sample("1", "-Inf")));
        assert!(queries_equal(
            &sample("1", "17.53656249999999"),
            &sample("1", "17.536562500000002")
        ));
        assert!(!queries_equal(&sample("1", "1.000002"), &sample("1", "1")));
        let timestamp_changed =
            json!({"data":{"result":[{"metric":{"instance":"1"},"value":[1.000_000_01,"1"]}]}});
        assert!(!queries_equal(&sample("1", "1"), &timestamp_changed));
        let string_result = |value| json!({"data":{"resultType":"string","result":[1,value]}});
        assert!(!queries_equal(&string_result("1"), &string_result("1.0")));
        let scalar =
            |timestamp, value| json!({"data":{"resultType":"scalar","result":[timestamp,value]}});
        assert!(queries_equal(
            &scalar(json!(1), "-2e-07"),
            &scalar(json!(1), "-0.0000002")
        ));
        assert!(!queries_equal(
            &scalar(json!(1), "1.000002"),
            &scalar(json!(1), "1")
        ));
        assert!(!queries_equal(
            &scalar(json!(1), "1"),
            &scalar(json!(2), "1")
        ));
        assert!(!queries_equal(
            &scalar(json!("1"), "1"),
            &scalar(json!("1.0"), "1")
        ));
        let one = json!({"metric":{"a":"x,b=y"},"value":[1,"1"]});
        let two = json!({"metric":{"a":"x","b":"y"},"value":[1,"1"]});
        assert!(queries_equal(
            &json!({"data":{"result":[one.clone(),two.clone()]}}),
            &json!({"data":{"result":[two,one]}})
        ));
    }
}
