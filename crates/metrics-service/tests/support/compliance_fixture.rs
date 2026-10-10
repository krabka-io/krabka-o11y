use krabka_metrics::wire::pb;
use serde::Deserialize;

use super::{FixtureLabel, SAMPLES_PER_BATCH, TestResult, remote_write_labels};

pub const END_MS: i64 = 1_750_000_000_000;
const START_MS: i64 = END_MS - 80 * 60 * 1_000;
const LAST_MS: i64 = END_MS + 10 * 60 * 1_000;

fn series(
    metric: &str,
    labels: &[FixtureLabel<'_>],
    value: impl Fn(i32) -> Option<f64>,
) -> pb::v1::TimeSeries {
    pb::v1::TimeSeries {
        labels: remote_write_labels(metric, labels.iter().copied()),
        samples: (START_MS..=LAST_MS)
            .step_by(5_000)
            .enumerate()
            .filter_map(|(tick, timestamp)| {
                value(i32::try_from(tick).expect("fixture tick fits i32"))
                    .map(|value| pb::v1::Sample { value, timestamp })
            })
            .collect(),
        ..Default::default()
    }
}

pub fn batches() -> Vec<Vec<u8>> {
    let mut timeseries = Vec::new();
    for instance in 0..3 {
        let address = format!("demo.promlabs.com:{}", 10_000 + instance);
        let labels = [
            FixtureLabel {
                name: "job",
                value: "demo",
            },
            FixtureLabel {
                name: "instance",
                value: address.as_str(),
            },
        ];
        let multiplier = f64::from(instance + 1);
        for (kind, base) in [
            ("free", 1_000_000.0),
            ("used", 3_000_000.0),
            ("cached", 500_000.0),
        ] {
            let mut labels = labels.to_vec();
            labels.push(FixtureLabel {
                name: "type",
                value: kind,
            });
            timeseries.push(series("demo_memory_usage_bytes", &labels, |tick| {
                Some(base * multiplier + f64::from(tick % 37) * 1_024.0)
            }));
        }
        timeseries.push(series("demo_num_cpus", &labels, |_| Some(2.0 * multiplier)));
        // Every ten minutes the counter resets; range queries cross resets.
        timeseries.push(series("demo_cpu_usage_seconds_total", &labels, |tick| {
            Some(f64::from(tick % 120) * multiplier)
        }));
        timeseries.push(series("demo_disk_usage_bytes", &labels, |tick| {
            Some(10_000_000.0 * multiplier + f64::from(tick) * 2_048.0)
        }));
        timeseries.push(series(
            "demo_batch_last_success_timestamp_seconds",
            &labels,
            |tick| Some(1_750_000_000.0 - 3_600.0 + f64::from(tick / 120) * 120.0),
        ));
        // A stale marker ends each one-minute burst, followed by nine minutes
        // without samples. The marker's exact NaN payload is the wire contract.
        timeseries.push(series("demo_intermittent_metric", &labels, |tick| {
            match tick % 120 {
                0..=11 => Some(multiplier),
                12 => Some(f64::from_bits(0x7ff0_0000_0000_0002)),
                _ => None,
            }
        }));
        for (bound, factor) in [("0.1", 1.0), ("0.5", 2.0), ("1", 3.0), ("+Inf", 4.0)] {
            let mut labels = labels.to_vec();
            labels.push(FixtureLabel {
                name: "le",
                value: bound,
            });
            timeseries.push(series(
                "demo_api_request_duration_seconds_bucket",
                &labels,
                |tick| Some(f64::from(tick % 120) * factor * multiplier),
            ));
        }
        for (metric, factor) in [
            ("demo_api_request_duration_seconds_sum", 1.75),
            ("demo_api_request_duration_seconds_count", 4.0),
        ] {
            timeseries.push(series(metric, &labels, |tick| {
                Some(f64::from(tick % 120) * factor * multiplier)
            }));
        }
    }
    let series = timeseries
        .into_iter()
        .map(|series| super::promql_corpus::CorpusSeries {
            labels: series
                .labels
                .into_iter()
                .map(|label| (label.name, label.value))
                .collect(),
            floats: series
                .samples
                .into_iter()
                .map(|sample| (sample.timestamp, sample.value))
                .collect(),
            histograms: Vec::new(),
        })
        .collect::<Vec<_>>();
    super::promql_corpus::remote_write_batches(&series, SAMPLES_PER_BATCH)
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ReportCounts {
    pub compared: usize,
    pub paired_errors: usize,
    pub skipped: usize,
    pub mismatched: usize,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Report {
    total_results: usize,
    results: Vec<Comparison>,
    include_passing: bool,
    query_tweaks: Option<Vec<serde_json::Value>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Comparison {
    test_case: Query,
    diff: String,
    unexpected_failure: String,
    unexpected_success: bool,
    unsupported: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Query {
    #[serde(rename = "query")]
    expression: String,
    skip_comparison: bool,
    should_fail: bool,
    start: String,
    end: String,
    resolution: i64,
}

pub fn report_counts(bytes: &[u8]) -> TestResult<ReportCounts> {
    let report: Report = serde_json::from_slice(bytes)?;
    if report.total_results == 0
        || report.results.len() != report.total_results
        || !report.include_passing
    {
        return Err("compliance report is empty or incomplete".into());
    }
    if report.query_tweaks.is_some_and(|tweaks| !tweaks.is_empty()) {
        return Err("compliance report applied query tweaks".into());
    }
    let mut counts = ReportCounts::default();
    for result in report.results {
        if result.test_case.expression.is_empty()
            || result.test_case.start.is_empty()
            || result.test_case.end.is_empty()
            || result.test_case.resolution <= 0
        {
            return Err("compliance report contains an invalid query case".into());
        }
        if !result.diff.is_empty()
            || !result.unexpected_failure.is_empty()
            || result.unexpected_success
            || result.unsupported
        {
            counts.mismatched += 1;
        } else if result.test_case.skip_comparison {
            counts.skipped += 1;
        } else if result.test_case.should_fail {
            counts.paired_errors += 1;
        } else {
            counts.compared += 1;
        }
    }
    Ok(counts)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use prost::Message;
    use serde_json::json;

    use super::*;

    #[test]
    fn fixture_contains_resets_staleness_and_all_catalogue_series() {
        let parts = batches()
            .into_iter()
            .flat_map(|batch| {
                let payload = snap::raw::Decoder::new().decompress_vec(&batch).unwrap();
                pb::v1::WriteRequest::decode(payload.as_slice())
                    .unwrap()
                    .timeseries
            })
            .collect::<Vec<_>>();
        let mut merged = std::collections::BTreeMap::new();
        for part in parts {
            let key = part
                .labels
                .iter()
                .map(|label| (label.name.clone(), label.value.clone()))
                .collect::<Vec<_>>();
            merged
                .entry(key)
                .or_insert_with(|| pb::v1::TimeSeries {
                    labels: part.labels.clone(),
                    ..Default::default()
                })
                .samples
                .extend(part.samples);
        }
        let series = merged.into_values().collect::<Vec<_>>();
        assert!(series.len() == 42);
        let metric = |series: &pb::v1::TimeSeries, name: &str| {
            series
                .labels
                .iter()
                .any(|label| label.name == "__name__" && label.value == name)
        };
        let counter = series
            .iter()
            .find(|series| metric(series, "demo_cpu_usage_seconds_total"))
            .unwrap();
        assert!(counter.samples.first().unwrap().timestamp == START_MS);
        assert!(counter.samples.last().unwrap().timestamp == LAST_MS);
        assert!(
            counter
                .samples
                .windows(2)
                .any(|pair| pair[1].value < pair[0].value)
        );
        let intermittent = series
            .iter()
            .find(|series| metric(series, "demo_intermittent_metric"))
            .unwrap();
        assert!(
            intermittent
                .samples
                .iter()
                .any(|sample| sample.value.to_bits() == 0x7ff0_0000_0000_0002)
        );
        assert!(
            intermittent
                .samples
                .windows(2)
                .any(|pair| pair[1].timestamp - pair[0].timestamp > 5 * 60 * 1_000)
        );
        let memory = series
            .iter()
            .filter(|series| metric(series, "demo_memory_usage_bytes"));
        let types = memory
            .flat_map(|series| {
                series
                    .labels
                    .iter()
                    .filter(|label| label.name == "type")
                    .map(|label| label.value.as_str())
            })
            .collect::<std::collections::BTreeSet<_>>();
        assert!(types == std::collections::BTreeSet::from(["cached", "free", "used"]));
    }

    #[test]
    fn reports_classify_errors_and_reject_empty_incomplete_or_malformed_runs() {
        let result = json!({
            "testCase": {"query": "up", "skipComparison": false, "shouldFail": false,
                "start": "2025-06-15T14:56:40Z", "end": "2025-06-15T15:06:40Z", "resolution": 10_000_000_000_i64},
            "diff": "", "unexpectedFailure": "", "unexpectedSuccess": false, "unsupported": false,
        });
        let mut report = json!({"totalResults": 1, "includePassing": true, "queryTweaks": null, "results": [result]});
        let parse =
            |report: &serde_json::Value| report_counts(&serde_json::to_vec(report).unwrap());
        assert!(
            parse(&report).unwrap()
                == ReportCounts {
                    compared: 1,
                    ..ReportCounts::default()
                }
        );
        report["results"][0]["testCase"]["shouldFail"] = json!(true);
        assert!(
            parse(&report).unwrap()
                == ReportCounts {
                    paired_errors: 1,
                    ..ReportCounts::default()
                }
        );
        report["results"][0]["testCase"]["skipComparison"] = json!(true);
        assert!(
            parse(&report).unwrap()
                == ReportCounts {
                    skipped: 1,
                    ..ReportCounts::default()
                }
        );
        report["results"][0]["testCase"]["skipComparison"] = json!(false);
        report["results"][0]["diff"] = json!("wrong samples");
        assert!(
            parse(&report).unwrap()
                == ReportCounts {
                    mismatched: 1,
                    ..ReportCounts::default()
                }
        );
        report["totalResults"] = json!(2);
        assert!(parse(&report).is_err());
        report["totalResults"] = json!(0);
        report["results"] = json!([]);
        assert!(parse(&report).is_err());
        assert!(report_counts(br#"{"totalResults":1,"results":[{}]}"#).is_err());
    }
}
