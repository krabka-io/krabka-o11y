use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};

use super::TestResult;

pub struct Fixture {
    pub metadata: Value,
    pub batches: Vec<Value>,
    pub end_ns: i64,
}

struct IndexKeys {
    index: &'static str,
    keys: Vec<&'static str>,
}

struct EventMarker {
    prefix: &'static str,
    status: &'static str,
}

#[derive(Debug, Eq, PartialEq)]
struct CatalogueCoverage {
    definitions: usize,
    metric_cases: usize,
    log_cases: usize,
}

impl CatalogueCoverage {
    fn total_cases(&self) -> usize {
        self.metric_cases + self.log_cases
    }
}

fn after_marker<'a>(line: &'a str, marker: &str) -> Option<&'a str> {
    line.find(marker).map(|index| &line[index + marker.len()..])
}

fn rfc3339(seconds: i64) -> TestResult<String> {
    let time = time::OffsetDateTime::from_unix_timestamp(seconds)?;
    Ok(format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        time.year(),
        u8::from(time.month()),
        time.day(),
        time.hour(),
        time.minute(),
        time.second()
    ))
}

fn entry(format: &str, timestamp: i64, tick: usize) -> Value {
    let level = ["debug", "INFO", "warn", "error"][tick % 4];
    let message = if level == "debug" {
        "debug duration level"
    } else {
        "duration error failed level refused"
    };
    let value = tick % 5 + 1;
    let line = if format == "json" {
        json!({"level": level, "msg": message, "error": "none", "duration": format!("{}ms", value * 10),
            "duration_ms": value * 10, "duration_seconds": 0.2 + f64::from(u32::try_from(value).unwrap()) / 10.0, "bytes": value * 100, "size": value * 20, "status": 200, "status_code": if level == "error" {500} else {200}}).to_string()
    } else {
        format!(
            "level={level} msg=\"{message}\" error=none duration={}ms duration_ms={} duration_seconds=0.5 bytes={} size={} status=200 status_code=500",
            value * 10,
            value * 10,
            value * 100,
            value * 20
        )
    };
    json!([timestamp.to_string(), line, {"detected_level": level.to_lowercase()}])
}

pub fn fixture(end_seconds: i64) -> TestResult<Fixture> {
    let start_seconds = end_seconds - 24 * 60 * 60;
    let mut metadata = json!({
        "version": "1.0", "all_selectors": [], "by_format": {}, "by_service_name": {},
        "by_unwrappable_field": {}, "by_detected_field": {}, "by_structured_metadata": {},
        "by_label_key": {}, "by_keyword": {}, "metadata_by_selector": {},
        "time_range": {"start": rfc3339(start_seconds)?, "end": rfc3339(end_seconds)?},
        "statistics": {"generated": rfc3339(end_seconds)?, "total_streams": 2,
            "streams_by_format": {"json": 1, "logfmt": 1}, "streams_by_service": {}},
    });
    let mut selectors = Vec::new();
    let mut batches = Vec::new();
    for format in ["json", "logfmt"] {
        let service = format!("fixture-{format}");
        let labels = json!({"cluster": "local", "container": format, "namespace": "conformance", "pod": service, "service_name": service});
        let selector = format!(
            "{{cluster=\"local\",container=\"{format}\",namespace=\"conformance\",pod=\"{service}\",service_name=\"{service}\"}}"
        );
        selectors.push(selector.clone());
        metadata["by_format"][format] = json!([selector]);
        metadata["by_service_name"][&service] = json!([selector]);
        metadata["statistics"]["streams_by_service"][&service] = json!(1);
        metadata["metadata_by_selector"][&selector] = json!({
            "min_range": 60_000_000_000_i64, "min_instant_range": 60_000_000_000_i64,
        });
        let values = (start_seconds..=end_seconds)
            .step_by(10)
            .enumerate()
            .map(|sample| entry(format, sample.1 * 1_000_000_000, sample.0))
            .collect::<Vec<_>>();
        batches.extend(
            values
                .chunks(512)
                .map(|values| json!({"streams": [{"stream": labels, "values": values}]})),
        );
    }
    metadata["all_selectors"] = json!(selectors);
    for index_keys in [
        IndexKeys {
            index: "by_label_key",
            keys: vec!["cluster", "container", "namespace", "pod", "service_name"],
        },
        IndexKeys {
            index: "by_keyword",
            keys: vec!["debug", "duration", "error", "failed", "level", "refused"],
        },
        IndexKeys {
            index: "by_detected_field",
            keys: vec![
                "level",
                "error",
                "msg",
                "duration",
                "duration_ms",
                "bytes",
                "size",
                "status",
                "status_code",
                "duration_seconds",
            ],
        },
        IndexKeys {
            index: "by_unwrappable_field",
            keys: vec![
                "duration",
                "duration_ms",
                "duration_seconds",
                "bytes",
                "size",
                "status",
                "status_code",
            ],
        },
        IndexKeys {
            index: "by_structured_metadata",
            keys: vec!["detected_level"],
        },
    ] {
        for key in index_keys.keys {
            metadata[index_keys.index][key] = json!(selectors);
        }
    }
    Ok(Fixture {
        metadata,
        batches,
        end_ns: end_seconds * 1_000_000_000,
    })
}

struct Events {
    loaded: usize,
    started: BTreeSet<String>,
    passed: BTreeSet<String>,
    failed: BTreeSet<String>,
    skipped: BTreeSet<String>,
    root_status: &'static str,
    metadata: BTreeMap<String, Value>,
    compared: BTreeSet<String>,
}

fn is_remote_leaf(name: &str) -> TestResult<bool> {
    if !name.starts_with("TestRemoteStorageEquality/") {
        return Ok(false);
    }
    let Some(kind_separator) = name.rfind("/kind=") else {
        return if name.contains("/direction=") {
            Err("malformed leaf classification".into())
        } else {
            Ok(false)
        };
    };
    let definition = &name[..kind_separator];
    let classification = &name[kind_separator + "/kind=".len()..];
    let Some(direction_separator) = classification.find("/direction=") else {
        return Err("malformed leaf classification".into());
    };
    let kind = &classification[..direction_separator];
    let direction = &classification[direction_separator + "/direction=".len()..];
    if definition == "TestRemoteStorageEquality/"
        || !matches!(kind, "metric" | "log")
        || !matches!(direction, "FORWARD" | "BACKWARD")
    {
        return Err("invalid remote leaf name".into());
    }
    Ok(true)
}

fn parse_events(output: &str, mode: &str) -> TestResult<Events> {
    let mut loaded = None;
    let mut started = BTreeSet::new();
    let mut passed = BTreeSet::new();
    let mut failed = BTreeSet::new();
    let mut skipped = BTreeSet::new();
    let mut root_status = None;
    let mut metadata = BTreeMap::new();
    let mut compared = BTreeSet::new();
    for line in output.lines().map(str::trim) {
        if let Some(encoded) = after_marker(line, "KRABKA_QUERY_CASE ") {
            let case: Value = serde_json::from_str(encoded)?;
            let name = case["name"]
                .as_str()
                .ok_or("case metadata needs test name")?;
            if !is_remote_leaf(name)?
                || case["query"]
                    .as_str()
                    .is_none_or(|query| query.trim().is_empty())
                || !matches!(
                    case["expected_outcome"].as_str(),
                    Some("query-result" | "semantic-negative")
                )
                || !name.contains(&format!(
                    "/kind={}/",
                    case["kind"].as_str().unwrap_or_default()
                ))
                || !name.ends_with(&format!(
                    "/direction={}",
                    case["direction"].as_str().unwrap_or_default()
                ))
            {
                return Err("invalid query case metadata".into());
            }
            let name = name.to_owned();
            if metadata.insert(name, case).is_some() {
                return Err("duplicate query case metadata".into());
            }
        }
        if let Some(name) = after_marker(line, "KRABKA_QUERY_VERDICT ")
            && (!is_remote_leaf(name)? || !compared.insert(name.to_owned()))
        {
            return Err("invalid or duplicate semantic comparison verdict".into());
        }
        if let Some(remaining) = after_marker(line, "Loaded ")
            && remaining.contains(&format!("remote test cases (range-type={mode})"))
        {
            let count = remaining
                .split_whitespace()
                .next()
                .ok_or("missing loaded case count")?
                .parse::<usize>()?;
            if loaded.replace(count).is_some() {
                return Err("duplicate loaded case event".into());
            }
        }
        for marker in [
            EventMarker {
                prefix: "=== RUN",
                status: "running",
            },
            EventMarker {
                prefix: "--- PASS:",
                status: "pass",
            },
            EventMarker {
                prefix: "--- FAIL:",
                status: "fail",
            },
            EventMarker {
                prefix: "--- SKIP:",
                status: "skip",
            },
        ] {
            let Some(remaining) = line.strip_prefix(marker.prefix) else {
                continue;
            };
            let name = remaining
                .split_whitespace()
                .next()
                .ok_or("missing Go test name")?;
            if name == "TestRemoteStorageEquality" {
                if marker.status != "running" && root_status.replace(marker.status).is_some() {
                    return Err("duplicate root completion event".into());
                }
                continue;
            }
            if !is_remote_leaf(name)? {
                continue;
            }
            let events = match marker.status {
                "running" => &mut started,
                "pass" => &mut passed,
                "fail" => &mut failed,
                "skip" => &mut skipped,
                _ => unreachable!("every marker has a known status"),
            };
            if !events.insert(name.to_string()) {
                return Err("duplicate test event".into());
            }
        }
    }
    let loaded = loaded.ok_or("remote suite did not load any named cases")?;
    let root_status = root_status.ok_or("remote suite did not finish its root test")?;
    let finished = passed
        .iter()
        .chain(&failed)
        .chain(&skipped)
        .cloned()
        .collect::<BTreeSet<_>>();
    if !passed.is_disjoint(&failed)
        || !passed.is_disjoint(&skipped)
        || !failed.is_disjoint(&skipped)
        || !finished.is_subset(&started)
        || started.len() > loaded
        || (root_status == "pass" && (started != passed || started.len() != loaded))
    {
        return Err("contradictory or incomplete Go test events".into());
    }
    if metadata.keys().cloned().collect::<BTreeSet<_>>() != started || compared != passed {
        return Err("case query metadata or semantic comparison verdict missing".into());
    }
    Ok(Events {
        loaded,
        started,
        passed,
        failed,
        skipped,
        root_status,
        metadata,
        compared,
    })
}

/// Reports named successful, failed, skipped and unfinished remote cases.
///
/// # Errors
/// Returns an error for malformed events or a different pinned catalogue.
/// Semantic failures remain structured reports with `success=false`.
pub fn execution_report(output: &str, mode: &str) -> TestResult<Value> {
    let expected = match mode {
        "range" => CatalogueCoverage {
            definitions: 101,
            metric_cases: 81,
            log_cases: 23,
        },
        "instant" => CatalogueCoverage {
            definitions: 81,
            metric_cases: 81,
            log_cases: 0,
        },
        _ => return Err("unknown remote query mode".into()),
    };
    let events = parse_events(output, mode)?;
    if events.loaded != expected.total_cases() {
        return Err(format!(
            "pinned catalogue expected {} cases, loaded {}",
            expected.total_cases(),
            events.loaded
        )
        .into());
    }
    let metric_cases = events
        .started
        .iter()
        .filter(|name| name.contains("/kind=metric/"))
        .count();
    let log_cases = events
        .started
        .iter()
        .filter(|name| name.contains("/kind=log/"))
        .count();
    let definitions = events
        .started
        .iter()
        .map(|name| name.split("/kind=").next().unwrap())
        .collect::<BTreeSet<_>>()
        .len();
    let actual = CatalogueCoverage {
        definitions,
        metric_cases,
        log_cases,
    };
    if events.started.len() == events.loaded && actual != expected {
        return Err(
            format!("pinned catalogue coverage expected {expected:?}, got {actual:?}").into(),
        );
    }
    let finished = events
        .passed
        .iter()
        .chain(&events.failed)
        .chain(&events.skipped)
        .cloned()
        .collect::<BTreeSet<_>>();
    let not_run = events
        .started
        .difference(&finished)
        .cloned()
        .collect::<BTreeSet<_>>();
    let success = events.root_status == "pass"
        && events.failed.is_empty()
        && events.skipped.is_empty()
        && events.passed.len() == events.loaded
        && actual == expected;
    let cases = events
        .metadata
        .into_iter()
        .map(|entry| {
            let name = entry.0;
            let mut case = entry.1;
            case["status"] = json!(if events.passed.contains(&name) {
                "passed"
            } else if events.failed.contains(&name) {
                "failed"
            } else if events.skipped.contains(&name) {
                "skipped"
            } else {
                "not_run"
            });
            case["semantic_comparison_completed"] = json!(events.compared.contains(&name));
            case
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "mode": mode, "success": success, "root_status": events.root_status,
        "original_declared_definitions": {"total": 101, "metrics": 73, "logs": 28, "marked_skipped_but_enabled": 15},
        "adapted_semantic_definitions": {"total": 101, "metrics": 81, "logs": 20, "inferred_metric_kinds": 8},
        "instant_log_definitions_not_applicable": if mode == "instant" {20} else {0},
        "expected": {"definitions": expected.definitions, "metric_cases": expected.metric_cases, "log_cases": expected.log_cases},
        "executed_definitions": definitions, "planned": events.loaded, "executed": finished.len(),
        "started_count": events.started.len(), "metric_cases": metric_cases, "log_cases": log_cases,
        "passed_count": events.passed.len(), "failed_count": events.failed.len(),
        "skipped_count": events.skipped.len(), "not_run_count": events.loaded - finished.len(),
        "unstarted_count": events.loaded - events.started.len(),
        "passed": events.passed, "failed": events.failed, "skipped": events.skipped, "not_run": not_run,
        "cases": cases,
    }))
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fmt::Write as _};

    use assert2::assert;

    use super::*;

    fn case_metadata(name: &str) -> String {
        format!(
            "KRABKA_QUERY_CASE {}\n",
            json!({
                "name":name, "query":"sum(count_over_time({app=\"fixture\"}[5m]))",
                "kind":"metric", "direction":"FORWARD", "expected_outcome":"query-result"
            })
        )
    }

    #[test]
    fn fixture_metadata_matches_formats_fields_and_last_instant_window() {
        let fixture = fixture(1_750_000_000).unwrap();
        let mut counts = BTreeMap::new();
        for batch in &fixture.batches {
            let stream = &batch["streams"][0];
            let format = stream["stream"]["container"].as_str().unwrap();
            *counts.entry(format).or_insert(0) += stream["values"].as_array().unwrap().len();
            let first = &stream["values"][0];
            assert!(first[2]["detected_level"].is_string());
            if format == "json" {
                let line: Value = serde_json::from_str(first[1].as_str().unwrap()).unwrap();
                assert!(line["duration"].as_str().unwrap().ends_with("ms"));
                assert!(line["duration_ms"].is_number() && line["bytes"].is_number());
            } else {
                assert!(first[1].as_str().unwrap().contains("duration="));
            }
        }
        let mut expected_counts = BTreeMap::new();
        expected_counts.insert("json", 8_641);
        expected_counts.insert("logfmt", 8_641);
        assert!(counts == expected_counts);
        let last = fixture.batches.last().unwrap()["streams"][0]["values"]
            .as_array()
            .unwrap()
            .last()
            .unwrap();
        assert!(last[0] == fixture.end_ns.to_string());
        assert!(fixture.metadata["all_selectors"].as_array().unwrap().len() == 2);
        assert!(
            fixture.metadata["by_keyword"]["debug"]
                .as_array()
                .unwrap()
                .len()
                == 2
        );
    }

    #[test]
    fn root_pass_without_cases_and_missing_failed_or_skipped_cases_fail() {
        let mut successful = "Loaded 81 remote test cases (range-type=instant)\n".to_string();
        for case in 0..81 {
            let name =
                format!("TestRemoteStorageEquality/query-{case}/kind=metric/direction=FORWARD");
            write!(
                successful,
                "=== RUN {name}\n{}KRABKA_QUERY_VERDICT {name}\n--- PASS: {name} (0s)\n",
                case_metadata(&name)
            )
            .unwrap();
        }
        successful.push_str("--- PASS: TestRemoteStorageEquality (0s)\n");
        assert!(execution_report(&successful, "instant").unwrap()["executed"] == 81);
        for invalid in [
            "--- PASS: TestRemoteStorageEquality (0s)\n".to_string(),
            successful.replace("Loaded 81", "Loaded 82"),
            successful.replace("KRABKA_QUERY_CASE", "MISSING_QUERY_CASE"),
            successful.replace("KRABKA_QUERY_VERDICT", "MISSING_QUERY_VERDICT"),
            successful.replace(
                "--- PASS: TestRemoteStorageEquality/query",
                "--- FAIL: TestRemoteStorageEquality/query",
            ),
            successful.replace(
                "--- PASS: TestRemoteStorageEquality/query",
                "--- SKIP: TestRemoteStorageEquality/query",
            ),
        ] {
            assert!(execution_report(&invalid, "instant").is_err());
        }
    }

    #[test]
    fn failed_leafs_keep_their_names_counts_and_unfinished_siblings() {
        let name =
            |case| format!("TestRemoteStorageEquality/query-{case}/kind=metric/direction=FORWARD");
        let mut events = "Loaded 81 remote test cases (range-type=instant)\n".to_string();
        for case in 0..81 {
            let name = name(case);
            write!(events, "=== RUN {name}\n{}", case_metadata(&name)).unwrap();
            match case {
                12 => writeln!(events, "--- FAIL: {name} (0s)").unwrap(),
                13 => writeln!(events, "--- SKIP: {name} (0s)").unwrap(),
                14 => (),
                _ => {
                    writeln!(events, "KRABKA_QUERY_VERDICT {name}\n--- PASS: {name} (0s)").unwrap();
                }
            }
        }
        events.push_str("--- FAIL: TestRemoteStorageEquality (0s)\nFAIL\n");
        let report = execution_report(&events, "instant").unwrap();
        assert!(report["success"] == false);
        assert!(report["passed_count"] == 78);
        assert!(report["failed_count"] == 1);
        assert!(report["skipped_count"] == 1);
        assert!(report["not_run_count"] == 1);
        assert!(report["executed"] == 80);
        assert!(report["failed"] == json!([name(12)]));
        assert!(report["skipped"] == json!([name(13)]));
        assert!(report["not_run"] == json!([name(14)]));
        assert!(
            !report["failed"]
                .as_array()
                .unwrap()
                .contains(&json!("TestRemoteStorageEquality"))
        );
    }
}
