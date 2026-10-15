use std::collections::BTreeMap;

use krabka_traceql::{MetricLabel, TraceMetricLabelType, TraceMetricSeries};

use super::*;

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
        Some(TraceMetricLabelType::Array) => {
            return json!({"key":key,"value":serde_json::from_str::<Value>(value).expect("array metric label is produced as OTLP JSON")});
        }
        Some(TraceMetricLabelType::Nil) | None => {}
    }

    if matches!(key, "p" | "__bucket")
        && let Ok(number) = value.parse::<f64>()
    {
        return json!({"key":key,"value":{"doubleValue":number}});
    }
    json!({ "key": key, "value": { "stringValue": value } })
}

#[cfg(test)]
mod metric_label_json_tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn array_labels_keep_order_and_element_types_instead_of_collapsing_to_first_value() {
        let ints = json!({"arrayValue":{"values":[{"intValue":"1"},{"intValue":"2"}]}});
        let strings = json!({"arrayValue":{"values":[{"stringValue":"1"},{"stringValue":"2"}]}});
        assert!(
            metric_label_json(
                "span.values",
                &ints.to_string(),
                Some(TraceMetricLabelType::Array)
            )["value"]
                == ints
        );
        assert!(
            metric_label_json(
                "span.values",
                &strings.to_string(),
                Some(TraceMetricLabelType::Array)
            )["value"]
                == strings
        );
        assert!(
            metric_label_json("span.values", "1", Some(TraceMetricLabelType::Int))["value"] != ints
        );
        assert!(ints != strings);
    }

    #[yare::parameterized(
        string_value = { "1", None, json!({"stringValue":"1"}) },
        int_value = { "1", Some(TraceMetricLabelType::Int), json!({"intValue":"1"}) },
        double_value = { "1", Some(TraceMetricLabelType::Double), json!({"doubleValue":1.0}) },
        bool_value = { "false", Some(TraceMetricLabelType::Bool), json!({"boolValue":false}) },
        nan_value = { "NaN", Some(TraceMetricLabelType::Double), json!({"doubleValue":"NaN"}) },
        infinity_value = { "inf", Some(TraceMetricLabelType::Double), json!({"doubleValue":"Infinity"}) },
    )]
    fn scalar_labels_preserve_anyvalue_type_and_nonfinite_doubles(
        text: &str,
        kind: Option<TraceMetricLabelType>,
        expected: Value,
    ) {
        ::core::assert_eq!(
            metric_label_json("span.mixed", text, kind),
            json!({"key":"span.mixed","value":expected})
        );
    }
}

/// Tempo's `Labels.String`: sorted verbatim names and typed display values.
pub(crate) fn metric_prom_labels(
    labels: &[MetricLabel],
    types: &BTreeMap<String, TraceMetricLabelType>,
) -> String {
    let mut labels = labels.iter().collect::<Vec<_>>();
    labels.sort_by(|left, right| left.key.cmp(&right.key));
    let inner = labels
        .into_iter()
        .map(|label| {
            let key = &label.key;
            let value = &label.value;
            let kind = types.get(key).copied().or_else(|| {
                matches!(key.as_str(), "p" | "__bucket").then_some(TraceMetricLabelType::Double)
            });
            let value = match kind {
                Some(TraceMetricLabelType::Int) => format!("int({value})"),
                Some(TraceMetricLabelType::Double) => {
                    let mut text = float_label(value.parse().expect("metric double is f64"));
                    if !text.contains(['e', '.']) {
                        text.push_str(".0");
                    }
                    text
                }
                Some(TraceMetricLabelType::Array) => array_label(value),
                Some(TraceMetricLabelType::Bool) => value.clone(),
                Some(TraceMetricLabelType::Nil) => "<nil>".into(),
                None => match value.as_str() {
                    "nil" => "<nil>".into(),
                    "" => "<empty>".into(),
                    _ => value.clone(),
                },
            };
            let mut characters = key.chars();
            let valid = characters
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
                && characters.all(|c| c.is_ascii_alphanumeric() || c == '_');
            let name = if valid { key.clone() } else { quote_label(key) };
            format!("{name}={}", quote_label(&value))
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("{{{inner}}}")
}

fn array_label(value: &str) -> String {
    let array: serde_json::Value = serde_json::from_str(value).expect("array label is OTLP JSON");
    let values = array["arrayValue"]["values"]
        .as_array()
        .expect("array label has values");
    let values = values
        .iter()
        .map(|value| {
            if let Some(value) = value.get("stringValue").and_then(serde_json::Value::as_str) {
                format!("`{value}`")
            } else if let Some(value) = value.get("intValue").and_then(serde_json::Value::as_str) {
                value.to_string()
            } else if let Some(value) = value.get("boolValue").and_then(serde_json::Value::as_bool)
            {
                value.to_string()
            } else {
                let value = &value["doubleValue"];
                float_label(value.as_f64().unwrap_or_else(|| match value.as_str() {
                    Some("NaN") => f64::NAN,
                    Some("Infinity") => f64::INFINITY,
                    Some("-Infinity") => f64::NEG_INFINITY,
                    _ => panic!("array double is f64"),
                }))
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{values}]")
}

// Go strconv.FormatFloat('g', -1, 64) uses exponent form outside [-4, 6).
fn float_label(value: f64) -> String {
    if value.is_nan() {
        return "NaN".into();
    }
    if value.is_infinite() {
        return if value.is_sign_negative() {
            "-Inf"
        } else {
            "+Inf"
        }
        .into();
    }
    let exponent = format!("{value:e}");
    let separator = exponent.find('e').expect("float exponent has e");
    let mantissa = &exponent[..separator];
    let power = &exponent[separator + 1..];
    let power = power.parse::<i32>().expect("float exponent is integer");
    if (-4..6).contains(&power) {
        value.to_string()
    } else {
        format!("{mantissa}e{power:+03}")
    }
}

fn quote_label(value: &str) -> String {
    static NONPRINTABLE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    // Go strconv.IsPrint admits Unicode L/M/N/P/S and ASCII space.
    let nonprintable = NONPRINTABLE
        .get_or_init(|| regex::Regex::new(r"[\p{C}\p{Z}]").expect("static Unicode pattern"));
    let mut output = String::from("\"");
    for character in value.chars() {
        match character {
            '\\' => output.push_str("\\\\"),
            '"' => output.push_str("\\\""),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            '\u{7}' => output.push_str("\\a"),
            '\u{8}' => output.push_str("\\b"),
            '\u{c}' => output.push_str("\\f"),
            '\u{b}' => output.push_str("\\v"),
            c if c.is_ascii_control() => {
                use std::fmt::Write as _;
                write!(output, "\\x{:02x}", u32::from(c)).expect("string writes are infallible");
            }
            c if c != ' ' && nonprintable.is_match(c.encode_utf8(&mut [0; 4])) => {
                use std::fmt::Write as _;
                let number = u32::from(c);
                if number < 256 {
                    write!(output, "\\x{number:02x}")
                } else if number < 65536 {
                    write!(output, "\\u{number:04x}")
                } else {
                    write!(output, "\\U{number:08x}")
                }
                .expect("string writes are infallible");
            }
            c => output.push(c),
        }
    }
    output.push('"');
    output
}

#[cfg(test)]
mod metric_prom_labels_tests {
    use assert2::assert;
    use krabka_traceql::MetricLabel;

    use super::{BTreeMap, TraceMetricLabelType, float_label, metric_prom_labels, quote_label};

    struct FloatLabelCase {
        value: f64,
        expected: &'static str,
    }

    #[test]
    fn legends_preserve_names_types_missing_values_and_go_escaping() {
        let labels = vec![
            MetricLabel::new("span.value", "1"),
            MetricLabel::new("span:duration", "1ms"),
            MetricLabel::new("empty", ""),
            MetricLabel::new("absent", "nil"),
            MetricLabel::new("text", "a\"\\\n\0"),
            MetricLabel::new("float", "1"),
        ];
        let mut types = BTreeMap::new();
        types.insert("span.value".into(), TraceMetricLabelType::Int);
        types.insert("float".into(), TraceMetricLabelType::Double);
        assert!(
            metric_prom_labels(&labels, &types)
                == r#"{absent="<nil>", empty="<empty>", float="1.0", "span.value"="int(1)", "span:duration"="1ms", text="a\"\\\n\x00"}"#
        );
        let mut strings = types.clone();
        strings.remove("span.value");
        assert!(metric_prom_labels(&labels, &strings) != metric_prom_labels(&labels, &types));
    }

    #[test]
    fn quotes_follow_go_unicode_printability() {
        assert!(
            quote_label("café \u{a0}\u{2028}\u{e000}\u{1d173}")
                == r#""café \xa0\u2028\ue000\U0001d173""#
        );
    }

    #[test]
    fn float_and_array_legends_follow_pinned_static_formatting() {
        for case in [
            FloatLabelCase {
                value: 1e6,
                expected: "1e+06",
            },
            FloatLabelCase {
                value: 1e-5,
                expected: "1e-05",
            },
            FloatLabelCase {
                value: 0.0001,
                expected: "0.0001",
            },
            FloatLabelCase {
                value: -0.0,
                expected: "-0",
            },
            FloatLabelCase {
                value: f64::INFINITY,
                expected: "+Inf",
            },
        ] {
            assert!(float_label(case.value) == case.expected);
        }
        let array =
            serde_json::json!({"arrayValue":{"values":[{"intValue":"2"},{"intValue":"4"}]}})
                .to_string();
        assert!(
            metric_prom_labels(&[MetricLabel::new("span.numbers", array)], &{
                let mut types = BTreeMap::new();
                types.insert("span.numbers".into(), TraceMetricLabelType::Array);
                types
            }) == r#"{"span.numbers"="[2, 4]"}"#
        );
    }
}

pub(crate) fn metric_value_json(value: f64) -> Value {
    if value.is_nan() {
        json!("NaN")
    } else if value == f64::INFINITY {
        json!("Infinity")
    } else if value == f64::NEG_INFINITY {
        json!("-Infinity")
    } else {
        json!(value)
    }
}

#[cfg(test)]
mod metric_value_json_tests {
    use super::metric_value_json;

    #[yare::parameterized(
        nan = { f64::NAN, "NaN" },
        positive_infinity = { f64::INFINITY, "Infinity" },
        negative_infinity = { f64::NEG_INFINITY, "-Infinity" },
    )]
    fn nonfinite_values_have_protojson_strings(value: f64, text: &str) {
        assert2::assert!(metric_value_json(value) == serde_json::json!(text));
    }

    #[test]
    fn finite_values_remain_json_numbers() {
        assert2::assert!(metric_value_json(0.0) == serde_json::json!(0.0));
    }
}

pub(crate) fn trace_metrics_json(resp: &TraceMetricsResponse, query: &str) -> Value {
    // Tempo `tempopb.QueryRangeResponse` protojson shape, which Grafana's Tempo
    // backend unmarshals: `series[].labels` is an ARRAY of KeyValue, samples use
    // `timestampMs` (milliseconds; int64 rendered as a string to match protojson)
    // and `value`. Krabka's internal point timestamps are nanoseconds.
    let name = metrics_operation_name(query);
    json!({
        "series": resp.series.iter().map(|series| {
            let name = series_name(series, name);
            let mut legend_labels = series.labels.clone();
            if let Some(name) = name {
                legend_labels.push(krabka_traceql::MetricLabel::new("__name__", name));
            }
            json!({
                "labels": metric_labels_json(series, name),
                "promLabels": metric_prom_labels(&legend_labels, &series.label_types),
                "samples": series.points.iter()
                    .map(|point| json!({
                        "timestampMs": (point.timestamp / 1_000_000).to_string(),
                        "value": metric_value_json(point.value),
                    }))
                    .collect::<Vec<_>>(),
                "exemplars": series.exemplars.iter()
                    .map(|exemplar| {
                        json!({
                            "labels": exemplar.labels.iter()
                                .map(|label| metric_label_json(&label.key, &label.value, exemplar.label_types.get(&label.key).copied()))
                                .collect::<Vec<_>>(),
                            "value": metric_value_json(if exemplar.value.is_nan() {
                                series.points.iter()
                                    .find(|point| point.timestamp >= exemplar.timestamp_ns)
                                    .map_or(exemplar.value, |point| point.value)
                            } else {
                                exemplar.value
                            }),
                            "timestampMs": (exemplar.timestamp_ns / 1_000_000).to_string(),
                        })
                    })
                    .collect::<Vec<_>>(),
            })
        }).collect::<Vec<_>>(),
    })
}

pub(crate) fn trace_metrics_instant_json(resp: &TraceMetricsResponse, query: &str) -> Value {
    let name = metrics_operation_name(query);
    super::instant_metrics_response(resp.series.iter().filter_map(|series| {
        series
            .points
            .first()
            .map(|point| super::InstantMetricSample {
                labels: metric_labels_json(series, series_name(series, name)),
                value: point.value,
            })
    }))
}

fn series_name(series: &TraceMetricSeries, name: Option<&'static str>) -> Option<&'static str> {
    name.filter(|_| {
        !series
            .labels
            .iter()
            .any(|label| matches!(label.key.as_str(), "__bucket" | "p"))
    })
}

fn metric_labels_json(series: &TraceMetricSeries, name: Option<&str>) -> Value {
    Value::Array(
        series
            .labels
            .iter()
            .map(|label| {
                metric_label_json(
                    &label.key,
                    &label.value,
                    series.label_types.get(&label.key).copied(),
                )
            })
            .chain(name.map(|name| metric_label_json("__name__", name, None)))
            .collect(),
    )
}

#[cfg(test)]
mod trace_metrics_json_tests {
    use std::collections::BTreeMap;

    use assert2::assert;
    use krabka_traceql::{MetricLabel, MetricPoint, TraceMetricExemplar, TraceMetricSeries};

    use super::*;

    #[test]
    fn instant_serialization_uses_first_sample_and_omits_sampleless_series() {
        let response = TraceMetricsResponse {
            series: vec![
                TraceMetricSeries {
                    label_types: BTreeMap::new(),
                    labels: Vec::new(),
                    points: vec![MetricPoint::new(1, 3.0), MetricPoint::new(2, 99.0)],
                    exemplars: Vec::new(),
                },
                TraceMetricSeries {
                    label_types: BTreeMap::new(),
                    labels: vec![MetricLabel::new("missing", "no-sample")],
                    points: Vec::new(),
                    exemplars: Vec::new(),
                },
            ],
        };
        assert!(
            trace_metrics_instant_json(&response, "{} | count_over_time()")
                == json!({
                    "series":[{"labels":[{"key":"__name__","value":{"stringValue":"count_over_time"}}],"value":3.0}],
                    "metrics":{"completedJobs":1,"totalJobs":1}
                })
        );
    }

    #[test]
    fn placeholder_exemplars_attach_to_right_closed_samples_and_keep_real_values() {
        let response = TraceMetricsResponse {
            series: vec![TraceMetricSeries {
                label_types: BTreeMap::default(),
                labels: Vec::new(),
                points: vec![
                    MetricPoint::new(20_000_000, 3.0),
                    MetricPoint::new(30_000_000, 7.0),
                ],
                exemplars: vec![
                    TraceMetricExemplar {
                        label_types: BTreeMap::default(),
                        labels: vec![MetricLabel::new("trace:id", "123")],
                        value: f64::NAN,
                        timestamp_ns: 20_000_000,
                    },
                    TraceMetricExemplar {
                        label_types: BTreeMap::default(),
                        labels: vec![MetricLabel::new("name", "selected")],
                        value: f64::NAN,
                        timestamp_ns: 21_000_000,
                    },
                    TraceMetricExemplar {
                        label_types: BTreeMap::default(),
                        labels: Vec::new(),
                        value: 0.5,
                        timestamp_ns: 22_000_000,
                    },
                ],
            }],
        };
        let actual = trace_metrics_json(&response, "{} | count_over_time()");
        assert!(
            actual["series"][0]["exemplars"]
                == json!([
                    { "labels": [{"key": "trace:id", "value": {"stringValue": "123"}}], "value": 3.0, "timestampMs": "20" },
                    { "labels": [{"key": "name", "value": {"stringValue": "selected"}}], "value": 7.0, "timestampMs": "21" },
                    { "labels": [], "value": 0.5, "timestampMs": "22" },
                ])
        );
    }
}
