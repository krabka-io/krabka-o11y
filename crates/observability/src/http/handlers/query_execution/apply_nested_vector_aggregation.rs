use std::collections::{BTreeMap, btree_map::Entry};

use crate::{
    HttpQueryError, Labels, Value, VectorAggregation, VectorAggregationOp, json,
    parse_metric_sample_value, querier::aggregate::sample_windows::vector_group_labels,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SampleTimestamp {
    numerator: i128,
    denominator: u128,
}

struct AggregatedSample {
    timestamp: Value,
    aggregation: FloatAggregation,
}

type TimestampStates = BTreeMap<SampleTimestamp, AggregatedSample>;

// Match the pinned Loki VectorAggEvaluator, including first-sample initialization.
struct FloatAggregation {
    count: f64,
    value: f64,
    mean: f64,
}

impl FloatAggregation {
    fn new(value: f64, op: &VectorAggregationOp) -> Self {
        Self {
            count: 1.0,
            value: if matches!(
                op,
                VectorAggregationOp::Stddev | VectorAggregationOp::Stdvar
            ) {
                0.0
            } else {
                value
            },
            mean: value,
        }
    }

    fn record(&mut self, value: f64, op: &VectorAggregationOp) -> Result<(), HttpQueryError> {
        match op {
            VectorAggregationOp::Sum => self.value += value,
            VectorAggregationOp::Count => self.count += 1.0,
            VectorAggregationOp::Min => {
                if self.value.is_nan() || value < self.value {
                    self.value = value;
                }
            }
            VectorAggregationOp::Max => {
                if self.value.is_nan() || value > self.value {
                    self.value = value;
                }
            }
            VectorAggregationOp::Avg => {
                self.count += 1.0;
                self.mean += (value - self.mean) / self.count;
            }
            VectorAggregationOp::Stddev | VectorAggregationOp::Stdvar => {
                self.count += 1.0;
                let delta = value - self.mean;
                self.mean += delta / self.count;
                self.value += delta * (value - self.mean);
            }
            _ => return Err(invalid_aggregation("unsupported nested aggregation")),
        }
        Ok(())
    }

    fn finish(self, op: &VectorAggregationOp) -> Result<f64, HttpQueryError> {
        Ok(match op {
            VectorAggregationOp::Sum | VectorAggregationOp::Min | VectorAggregationOp::Max => {
                self.value
            }
            VectorAggregationOp::Count => self.count,
            VectorAggregationOp::Avg => self.mean,
            VectorAggregationOp::Stdvar => self.value / self.count,
            VectorAggregationOp::Stddev => (self.value / self.count).sqrt(),
            _ => return Err(invalid_aggregation("unsupported nested aggregation")),
        })
    }
}

pub(crate) fn format_float_sample(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value.is_infinite() {
        if value.is_sign_negative() {
            "-Inf"
        } else {
            "+Inf"
        }
        .to_string()
    } else {
        value.to_string()
    }
}

pub(crate) fn apply_nested_vector_aggregation(
    response: &mut Value,
    aggregation: &VectorAggregation,
) -> Result<(), HttpQueryError> {
    if !matches!(
        aggregation.op,
        VectorAggregationOp::Sum
            | VectorAggregationOp::Count
            | VectorAggregationOp::Min
            | VectorAggregationOp::Max
            | VectorAggregationOp::Avg
            | VectorAggregationOp::Stddev
            | VectorAggregationOp::Stdvar
    ) {
        return Err(invalid_aggregation("unsupported nested aggregation"));
    }
    let is_vector = match response.pointer("/data/resultType").and_then(Value::as_str) {
        Some("vector") => true,
        Some("matrix") => false,
        _ => {
            return Err(invalid_aggregation(
                "aggregation requires a vector or matrix",
            ));
        }
    };
    let series = response
        .pointer("/data/result")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid_aggregation("missing metric series"))?;
    let mut groups = BTreeMap::<Labels, TimestampStates>::new();
    for item in series {
        let labels: Labels = serde_json::from_value(item["metric"].clone())
            .map_err(|_| invalid_aggregation("invalid metric labels"))?;
        let labels = vector_group_labels(&labels, aggregation.grouping.as_ref());
        let samples = if is_vector {
            std::slice::from_ref(&item["value"])
        } else {
            item["values"]
                .as_array()
                .ok_or_else(|| invalid_aggregation("missing matrix samples"))?
        };
        for sample in samples {
            record_sample(
                groups.entry(labels.clone()).or_default(),
                sample,
                &aggregation.op,
            )?;
        }
    }
    let mut result = Vec::new();
    for labels in groups.keys().cloned().collect::<Vec<_>>() {
        let states = groups.remove(&labels).expect("selected group exists");
        let mut samples = states
            .into_values()
            .map(|state| {
                Ok(json!([
                    state.timestamp,
                    format_float_sample(state.aggregation.finish(&aggregation.op)?)
                ]))
            })
            .collect::<Result<Vec<_>, HttpQueryError>>()?;
        samples.sort_by(|left, right| {
            left[0]
                .as_f64()
                .expect("validated timestamp")
                .total_cmp(&right[0].as_f64().expect("validated timestamp"))
        });
        if is_vector {
            if samples.len() != 1 {
                return Err(invalid_aggregation("instant sample timestamps differ"));
            }
            result.push(json!({"metric": labels, "value": samples.remove(0)}));
        } else if !samples.is_empty() {
            result.push(json!({"metric": labels, "values": samples}));
        }
    }
    response["data"]["result"] = json!(result);
    Ok(())
}

fn record_sample(
    states: &mut TimestampStates,
    sample: &Value,
    op: &VectorAggregationOp,
) -> Result<(), HttpQueryError> {
    let parts = sample
        .as_array()
        .filter(|parts| parts.len() == 2)
        .ok_or_else(|| invalid_aggregation("invalid metric sample"))?;
    if !parts[0].is_number() || !parts[0].as_f64().is_some_and(f64::is_finite) {
        return Err(invalid_aggregation("invalid metric timestamp"));
    }
    let timestamp = parse_metric_sample_value(&parts[0].to_string())
        .ok_or_else(|| invalid_aggregation("metric timestamp is not representable"))?;
    let text = parts[1]
        .as_str()
        .ok_or_else(|| invalid_aggregation("invalid metric sample value"))?;
    let value = match text {
        "NaN" => f64::NAN,
        "+Inf" | "Inf" => f64::INFINITY,
        "-Inf" => f64::NEG_INFINITY,
        _ => text
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite())
            .ok_or_else(|| invalid_aggregation("invalid metric sample value"))?,
    };
    match states.entry(SampleTimestamp {
        numerator: timestamp.numerator,
        denominator: timestamp.denominator,
    }) {
        Entry::Vacant(entry) => {
            entry.insert(AggregatedSample {
                timestamp: parts[0].clone(),
                aggregation: FloatAggregation::new(value, op),
            });
        }
        Entry::Occupied(mut entry) => entry.get_mut().aggregation.record(value, op)?,
    }
    Ok(())
}

fn invalid_aggregation(message: &str) -> HttpQueryError {
    HttpQueryError::LokiFormatPlainParse(message.to_string())
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_logql::VectorGrouping;

    use super::*;

    struct InstantAggregationCase {
        op: VectorAggregationOp,
        api: &'static str,
        cache: &'static str,
    }

    struct FloatAggregationCase {
        op: VectorAggregationOp,
        samples: Vec<&'static str>,
        expected: &'static str,
    }

    #[test]
    fn nested_max_of_grouped_averages_keeps_each_timestamp_separate() {
        let mut response = json!({"status": "success", "data": {
            "resultType": "matrix", "stats": {"summary": {"totalBytesProcessed": 42}},
            "result": [
                {"metric": {"service": "api", "instance": "a"}, "values": [[0, "1"], [10, "7"]]},
                {"metric": {"service": "api", "instance": "b"}, "values": [[0, "3"], [10, "9"]]},
                {"metric": {"service": "cache", "instance": "a"}, "values": [[0, "0"], [10, "2"]]},
                {"metric": {"service": "cache", "instance": "b"}, "values": [[0, "2"], [10, "4"]]}
            ]
        }});
        apply_nested_vector_aggregation(
            &mut response,
            &VectorAggregation {
                op: VectorAggregationOp::Avg,
                grouping: Some(VectorGrouping::By(vec!["service".into()])),
            },
        )
        .unwrap();
        assert!(
            response["data"]["result"]
                == json!([
                    {"metric": {"service": "api"}, "values": [[0, "2"], [10, "8"]]},
                    {"metric": {"service": "cache"}, "values": [[0, "1"], [10, "3"]]}
                ])
        );
        apply_nested_vector_aggregation(
            &mut response,
            &VectorAggregation {
                op: VectorAggregationOp::Max,
                grouping: None,
            },
        )
        .unwrap();
        assert!(
            response
                == json!({"status": "success", "data": {
                    "resultType": "matrix", "stats": {"summary": {"totalBytesProcessed": 42}},
                    "result": [{"metric": {}, "values": [[0, "2"], [10, "8"]]}]
                }})
        );
    }

    #[test]
    fn all_reductions_use_grouped_instant_samples() {
        let response = json!({"data": {"resultType": "vector", "result": [
            {"metric": {"service": "api", "instance": "a"}, "value": [2, "1"]},
            {"metric": {"service": "api", "instance": "b"}, "value": [2.0, "3e0"]},
            {"metric": {"service": "cache", "instance": "a"}, "value": [2, "5"]}
        ]}});
        for case in [
            InstantAggregationCase {
                op: VectorAggregationOp::Sum,
                api: "4",
                cache: "5",
            },
            InstantAggregationCase {
                op: VectorAggregationOp::Count,
                api: "2",
                cache: "1",
            },
            InstantAggregationCase {
                op: VectorAggregationOp::Min,
                api: "1",
                cache: "5",
            },
            InstantAggregationCase {
                op: VectorAggregationOp::Max,
                api: "3",
                cache: "5",
            },
            InstantAggregationCase {
                op: VectorAggregationOp::Avg,
                api: "2",
                cache: "5",
            },
            InstantAggregationCase {
                op: VectorAggregationOp::Stddev,
                api: "1",
                cache: "0",
            },
            InstantAggregationCase {
                op: VectorAggregationOp::Stdvar,
                api: "1",
                cache: "0",
            },
        ] {
            let mut actual = response.clone();
            apply_nested_vector_aggregation(
                &mut actual,
                &VectorAggregation {
                    op: case.op,
                    grouping: Some(VectorGrouping::Without(vec!["instance".into()])),
                },
            )
            .unwrap();
            assert!(
                actual
                    == json!({"data": {"resultType": "vector", "result": [
                        {"metric": {"service": "api"}, "value": [2, case.api]},
                        {"metric": {"service": "cache"}, "value": [2, case.cache]}
                    ]}})
            );
        }
    }

    #[test]
    fn invalid_samples_error_without_changing_the_response() {
        let response = json!({"data": {"resultType": "matrix", "result": [
            {"metric": {"service": "api"}, "values": [[0, "2"], [10, "4"]]}
        ]}});
        let aggregation = VectorAggregation {
            op: VectorAggregationOp::Sum,
            grouping: None,
        };
        for sample in [
            json!([10, "bad"]),
            json!([10, null]),
            json!([null, "4"]),
            json!(["10", "4"]),
            json!([10]),
            json!([10, "1e400"]),
        ] {
            let mut actual = response.clone();
            actual["data"]["result"][0]["values"][1] = sample;
            let before = actual.clone();
            assert!(apply_nested_vector_aggregation(&mut actual, &aggregation).is_err());
            assert!(actual == before);
        }
        let mut actual = response;
        assert!(
            apply_nested_vector_aggregation(
                &mut actual,
                &VectorAggregation {
                    op: VectorAggregationOp::TopK(1),
                    grouping: None
                }
            )
            .is_err()
        );
    }

    #[test]
    fn float_reductions_match_lokis_nonfinite_and_large_value_rules() {
        for case in [
            FloatAggregationCase {
                op: VectorAggregationOp::Sum,
                samples: vec!["+Inf", "1"],
                expected: "+Inf",
            },
            FloatAggregationCase {
                op: VectorAggregationOp::Sum,
                samples: vec!["+Inf", "-Inf"],
                expected: "NaN",
            },
            FloatAggregationCase {
                op: VectorAggregationOp::Sum,
                samples: vec!["1e308", "1e308"],
                expected: "+Inf",
            },
            FloatAggregationCase {
                op: VectorAggregationOp::Sum,
                samples: vec!["-0", "-0"],
                expected: "-0",
            },
            FloatAggregationCase {
                op: VectorAggregationOp::Avg,
                samples: vec!["+Inf", "1"],
                expected: "NaN",
            },
            FloatAggregationCase {
                op: VectorAggregationOp::Avg,
                samples: vec!["1", "+Inf"],
                expected: "+Inf",
            },
            FloatAggregationCase {
                op: VectorAggregationOp::Avg,
                samples: vec!["NaN", "1"],
                expected: "NaN",
            },
            FloatAggregationCase {
                op: VectorAggregationOp::Count,
                samples: vec!["NaN", "+Inf", "-Inf"],
                expected: "3",
            },
            FloatAggregationCase {
                op: VectorAggregationOp::Min,
                samples: vec!["NaN", "3", "+Inf"],
                expected: "3",
            },
            FloatAggregationCase {
                op: VectorAggregationOp::Max,
                samples: vec!["-Inf", "NaN", "3"],
                expected: "3",
            },
            FloatAggregationCase {
                op: VectorAggregationOp::Min,
                samples: vec!["NaN", "NaN"],
                expected: "NaN",
            },
            FloatAggregationCase {
                op: VectorAggregationOp::Stdvar,
                samples: vec!["+Inf"],
                expected: "0",
            },
            FloatAggregationCase {
                op: VectorAggregationOp::Stdvar,
                samples: vec!["+Inf", "1"],
                expected: "NaN",
            },
            FloatAggregationCase {
                op: VectorAggregationOp::Stddev,
                samples: vec!["NaN", "1"],
                expected: "NaN",
            },
            FloatAggregationCase {
                op: VectorAggregationOp::Stdvar,
                samples: vec!["1000000000000", "1000000000002"],
                expected: "1",
            },
            FloatAggregationCase {
                op: VectorAggregationOp::Stddev,
                samples: vec!["1000000000000", "1000000000002"],
                expected: "1",
            },
            FloatAggregationCase {
                op: VectorAggregationOp::Min,
                samples: vec!["1e20", "2e20"],
                expected: "100000000000000000000",
            },
            FloatAggregationCase {
                op: VectorAggregationOp::Max,
                samples: vec!["1e20", "2e20"],
                expected: "200000000000000000000",
            },
            FloatAggregationCase {
                op: VectorAggregationOp::Avg,
                samples: vec!["1e20", "1e20"],
                expected: "100000000000000000000",
            },
        ] {
            let mut response = json!({"status": "success", "data": {
                "resultType": "vector", "result": case.samples.iter().map(|sample|
                    json!({"metric": {}, "value": [10, sample]})
                ).collect::<Vec<_>>()
            }});
            apply_nested_vector_aggregation(
                &mut response,
                &VectorAggregation {
                    op: case.op,
                    grouping: None,
                },
            )
            .unwrap();
            assert!(
                response
                    == json!({"status": "success", "data": {
                        "resultType": "vector", "result": [{"metric": {}, "value": [10, case.expected]}]
                    }})
            );
        }
    }
}
