//! Record the pinned Grafana Loki plugin's requests and replay them unchanged.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use assert2::assert;
use axum::{
    Router,
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::oneshot};

use super::{KRABKA_UID, LOKI_UID, TestResult, Timeline, canonical_labels, dataset, normalize};

type Requests = Arc<Mutex<Vec<Value>>>;

#[derive(Clone)]
struct ProxyState {
    upstream: String,
    client: reqwest::Client,
    requests: Requests,
}

pub struct RecordingProxy {
    pub port: u16,
    requests: Requests,
    shutdown: Option<oneshot::Sender<()>>,
}

impl Drop for RecordingProxy {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

impl RecordingProxy {
    pub async fn start(upstream: String) -> TestResult<Self> {
        let listener = TcpListener::bind("0.0.0.0:0").await?;
        let port = listener.local_addr()?.port();
        let requests = Requests::default();
        let state = ProxyState {
            upstream,
            client: reqwest::Client::new(),
            requests: requests.clone(),
        };
        let (shutdown, stop) = oneshot::channel();
        tokio::spawn(async move {
            let _ = axum::serve(listener, Router::new().fallback(forward).with_state(state))
                .with_graceful_shutdown(async move {
                    let _ = stop.await;
                })
                .await;
        });
        Ok(Self {
            port,
            requests,
            shutdown: Some(shutdown),
        })
    }

    fn take(&self) -> TestResult<Vec<Value>> {
        Ok(std::mem::take(
            &mut *self
                .requests
                .lock()
                .map_err(|_| "request recorder poisoned")?,
        ))
    }
}

async fn forward(State(state): State<ProxyState>, request: Request<Body>) -> Response {
    match forward_request(state, request).await {
        Ok(response) => response,
        Err(error) => (StatusCode::BAD_GATEWAY, error).into_response(),
    }
}

async fn forward_request(state: ProxyState, request: Request<Body>) -> Result<Response, String> {
    let (parts, body) = request.into_parts();
    let body = axum::body::to_bytes(body, 1024 * 1024)
        .await
        .map_err(|error| error.to_string())?;
    let tenant = parts
        .headers
        .get("X-Scope-OrgID")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    state
        .requests
        .lock()
        .map_err(|_| "request recorder poisoned")?
        .push(json!({
            "method":parts.method.as_str(), "uri":parts.uri.to_string(), "tenant":tenant,
            "body":String::from_utf8_lossy(&body),
        }));
    let mut outgoing = state
        .client
        .request(parts.method, format!("{}{}", state.upstream, parts.uri))
        .header("X-Scope-OrgID", tenant)
        .body(body);
    if let Some(content_type) = parts.headers.get("content-type") {
        outgoing = outgoing.header("content-type", content_type);
    }
    let response = outgoing.send().await.map_err(|error| error.to_string())?;
    let mut result = Response::builder().status(response.status());
    if let Some(content_type) = response.headers().get("content-type") {
        result = result.header("content-type", content_type);
    }
    result
        .body(Body::from(
            response.bytes().await.map_err(|error| error.to_string())?,
        ))
        .map_err(|error| error.to_string())
}

struct ReplayCase {
    id: &'static str,
    expression: &'static str,
    query_type: &'static str,
    direction: &'static str,
    limit: usize,
    offsets: &'static [i64],
}

const CASES: &[ReplayCase] = &[
    ReplayCase {
        id: "logs-forward",
        expression: r#"{app="db"}"#,
        query_type: "range",
        direction: "forward",
        limit: 100,
        offsets: &[8, 28, 48],
    },
    ReplayCase {
        id: "logs-backward",
        expression: r#"{app="db"}"#,
        query_type: "range",
        direction: "backward",
        limit: 100,
        offsets: &[48, 28, 8],
    },
    ReplayCase {
        id: "logs-filter",
        expression: r#"{app="db"} |= "ERROR""#,
        query_type: "range",
        direction: "forward",
        limit: 100,
        offsets: &[28],
    },
    ReplayCase {
        id: "logs-limit",
        expression: r#"{app="db"}"#,
        query_type: "range",
        direction: "backward",
        limit: 1,
        offsets: &[48],
    },
    ReplayCase {
        id: "metric-instant",
        expression: r#"sum(count_over_time({app="db"}[5m]))"#,
        query_type: "instant",
        direction: "forward",
        limit: 100,
        offsets: &[],
    },
];

fn dashboard_request(case: &ReplayCase, timeline: &Timeline, uid: &str) -> Value {
    json!({"from":(timeline.start / 1_000_000).to_string(),"to":(timeline.end / 1_000_000).to_string(),
        "queries":[{"refId":"A","datasource":{"type":"loki","uid":uid},
            "expr":case.expression,"queryType":case.query_type,"direction":case.direction,
            "maxLines":case.limit,"intervalMs":15000,"maxDataPoints":1000}]})
}

fn expected_logs(case: &ReplayCase, timeline: &Timeline) -> Value {
    let payload = dataset(timeline);
    let values = payload["streams"][2]["values"]
        .as_array()
        .expect("db fixture entries");
    let mut streams = Vec::new();
    for level in ["error", "info"] {
        let entries: Vec<_> = case
            .offsets
            .iter()
            .filter(|offset| (**offset == 28) == (level == "error"))
            .map(|offset| {
                values
                    .iter()
                    .find(|entry| {
                        entry[0].as_str().and_then(|text| text.parse::<i64>().ok())
                            == Some(timeline.at(*offset))
                    })
                    .expect("independent fixture offset")
                    .clone()
            })
            .collect();
        if !entries.is_empty() {
            streams.push(json!({"stream":{"app":"db","env":"staging","service_name":"db","detected_level":level},"values":entries}));
        }
    }
    normalize(&json!({"data":{"resultType":"streams","result":streams}}))
}

fn check_answer(case: &ReplayCase, timeline: &Timeline, answer: &Value) -> TestResult<Value> {
    if case.query_type == "instant" {
        let entries = answer["result"].as_array().ok_or("metric result missing")?;
        if answer["resultType"] != "vector"
            || entries.len() != 1
            || entries[0]["metric"] != json!({})
            || entries[0]["values"]
                .as_array()
                .is_none_or(|values| values.len() != 1)
            || entries[0]["values"][0][0] != timeline.end / 1_000_000_000
            || entries[0]["values"][0][1] != "3"
        {
            return Err(
                format!("instant metric differs from three-entry input ledger: {answer}").into(),
            );
        }
        Ok(json!({"resultType":"vector","metric":{},"value":3}))
    } else {
        let expected = expected_logs(case, timeline);
        if *answer != expected {
            return Err(format!(
                "log result differs from independent entries: expected={expected}; actual={answer}"
            )
            .into());
        }
        Ok(expected)
    }
}

async fn replay(client: &reqwest::Client, backend: &str, request: &Value) -> TestResult<Value> {
    assert!(request["method"] == "GET" && request["body"] == "" && request["tenant"] == "tenant-a");
    let uri = request["uri"].as_str().ok_or("recorded URI missing")?;
    let response = client
        .get(format!("{backend}{uri}"))
        .header("X-Scope-OrgID", "tenant-a")
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;
    let mut result = normalize(&response);
    if response["data"]["resultType"] == "vector" {
        let entries = response["data"]["result"]
            .as_array()
            .ok_or("instant entries missing")?;
        let samples: Vec<_> = entries
            .iter()
            .map(|entry| {
                let text = entry["value"][1].as_str().ok_or("instant value missing")?;
                // Parse formatting differences without rounding away a changed count.
                let value = text.parse::<f64>()?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(json!({"metric":entry["metric"],
                "values":[[entry["value"][0], value.to_string()]]}))
            })
            .collect::<TestResult<_>>()?;
        result["result"] = json!(samples);
    }
    Ok(result)
}

/// Keep schema types, labels and every value; omit Grafana's execution metadata.
fn frames(response: &Value) -> TestResult<Value> {
    let frames = response["results"]["A"]["frames"]
        .as_array()
        .ok_or("Grafana data frames missing")?;
    if frames.is_empty() || response["results"]["A"].get("error").is_some() {
        return Err(format!("Grafana query failed or returned no frames: {response}").into());
    }
    let mut result: Vec<_> = frames
        .iter()
        .map(canonical_frame)
        .collect::<TestResult<_>>()?;
    result.sort_by_key(Value::to_string);
    Ok(json!(result))
}

fn canonical_frame(frame: &Value) -> TestResult<Value> {
    let fields = frame["schema"]["fields"]
        .as_array()
        .ok_or("frame schema missing")?;
    let mut data = frame["data"].clone();
    if let Some(label_index) = fields.iter().position(|field| field["name"] == "labels") {
        let columns = data["values"].as_array().ok_or("frame data missing")?;
        let count = columns
            .get(label_index)
            .and_then(Value::as_array)
            .ok_or("frame labels missing")?
            .len();
        if columns.len() != fields.len()
            || columns
                .iter()
                .any(|column| column.as_array().is_none_or(|column| column.len() != count))
        {
            return Err("frame columns have different lengths".into());
        }
        // Loki does not order streams. Keep each stream's entry order and all columns.
        let mut streams: BTreeMap<String, Vec<Vec<Value>>> = BTreeMap::new();
        for index in 0..count {
            streams
                .entry(canonical_labels(&columns[label_index][index]))
                .or_default()
                .push(columns.iter().map(|column| column[index].clone()).collect());
        }
        let rows: Vec<_> = streams.into_values().flatten().collect();
        data["values"] = json!(
            (0..fields.len())
                .map(|index| rows
                    .iter()
                    .map(|row| row[index].clone())
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>()
        );
    }
    let mut meta = frame["schema"]
        .get("meta")
        .cloned()
        .unwrap_or_else(|| json!({}));
    if let Some(meta) = meta.as_object_mut() {
        meta.remove("stats");
        meta.remove("executedQueryString");
    }
    Ok(json!({"fields":fields,"data":data,"meta":meta}))
}

fn check_frames(case: &ReplayCase, timeline: &Timeline, response: &Value) -> TestResult<Value> {
    let result = frames(response)?;
    let mut observed = Vec::new();
    for frame in result.as_array().ok_or("normalized frames missing")? {
        let fields = frame["fields"].as_array().ok_or("frame fields missing")?;
        let columns = frame["data"]["values"]
            .as_array()
            .ok_or("frame columns missing")?;
        if columns.len() != fields.len() {
            return Err("frame columns differ from schema".into());
        }
        let name = if case.query_type == "instant" {
            "Value"
        } else {
            "Line"
        };
        let index = fields
            .iter()
            .position(|field| {
                field["name"]
                    .as_str()
                    .is_some_and(|field| field.eq_ignore_ascii_case(name))
            })
            .ok_or("frame value column missing")?;
        let values = columns[index].as_array().ok_or("frame values missing")?;
        if case.query_type == "instant" {
            let time_index = fields
                .iter()
                .position(|field| field["type"] == "time")
                .ok_or("metric frame time column missing")?;
            if columns[time_index] != json!([timeline.end / 1_000_000]) {
                return Err("metric frame timestamp differs from requested endpoint".into());
            }
            observed.extend(values.iter().cloned());
        } else {
            let time_index = fields
                .iter()
                .position(|field| field["type"] == "time")
                .ok_or("frame time column missing")?;
            let times = columns[time_index]
                .as_array()
                .ok_or("frame timestamps missing")?;
            if times.len() != values.len() {
                return Err("frame timestamp/value lengths differ".into());
            }
            observed.extend(
                times
                    .iter()
                    .zip(values)
                    .map(|(time, line)| json!([time, line])),
            );
        }
    }
    let mut expected = if case.query_type == "instant" {
        vec![json!(3)]
    } else {
        let payload = dataset(timeline);
        case.offsets
            .iter()
            .map(|offset| {
                let line = payload["streams"][2]["values"]
                    .as_array()
                    .expect("fixture entries")
                    .iter()
                    .find(|entry| {
                        entry[0].as_str().and_then(|text| text.parse::<i64>().ok())
                            == Some(timeline.at(*offset))
                    })
                    .expect("fixture offset")[1]
                    .clone();
                json!([timeline.at(*offset) / 1_000_000, line])
            })
            .collect()
    };
    observed.sort_by_key(Value::to_string);
    expected.sort_by_key(Value::to_string);
    if observed != expected {
        return Err(format!("Grafana frame values differ from independent input ledger: expected={expected:?}, actual={observed:?}").into());
    }
    Ok(result)
}

pub async fn run(
    client: &reqwest::Client,
    grafana: &str,
    timeline: &Timeline,
    oracle: &RecordingProxy,
    candidate: &RecordingProxy,
    oracle_backend: &str,
    candidate_backend: &str,
) -> TestResult {
    let mut report = json!({"suite":"grafana-loki-client-replay","status":"running","planned":CASES.len(),
        "grafana_image_tag":std::env::var("KRABKA_GRAFANA_IMAGE_TAG").ok(),
        "loki_image_tag":std::env::var("KRABKA_LOKI_IMAGE_TAG").ok(),
        "timeline_base_ns":timeline.base,"fixture":dataset(timeline),"cases":[]});
    let mut failures = Vec::new();
    for case in CASES {
        oracle.take()?;
        candidate.take()?;
        let oracle_input = dashboard_request(case, timeline, LOKI_UID);
        let candidate_input = dashboard_request(case, timeline, KRABKA_UID);
        let mut responses = Vec::new();
        for input in [&oracle_input, &candidate_input] {
            let response = client
                .post(format!("{grafana}/api/ds/query"))
                .json(input)
                .send()
                .await?;
            responses.push(
                json!({"status":response.status().as_u16(),"body":response.json::<Value>().await?}),
            );
        }
        let oracle_requests = oracle.take()?;
        let candidate_requests = candidate.take()?;
        let mut row = json!({"id":case.id,"expression":case.expression,"dashboard_requests":{"oracle":oracle_input,"candidate":candidate_input},
            "generated_requests":{"oracle":oracle_requests,"candidate":candidate_requests},
            "grafana_responses":{"oracle":responses[0],"candidate":responses[1]},"status":"failed"});
        let result = async {
            if oracle_requests.len() != 1 || oracle_requests != candidate_requests {
                return Err(format!("Grafana generated different requests: {oracle_requests:?} {candidate_requests:?}").into());
            }
            let expected = replay(client, oracle_backend, &oracle_requests[0]).await?;
            let actual = replay(client, candidate_backend, &oracle_requests[0]).await?;
            row["oracle"] = expected.clone();
            row["candidate"] = actual.clone();
            row["independent_expected"] = check_answer(case, timeline, &expected)?;
            check_answer(case, timeline, &actual)?;
            if expected != actual || responses.iter().any(|response| response["status"] != 200)
                || check_frames(case, timeline, &responses[0]["body"])? != check_frames(case, timeline, &responses[1]["body"])? {
                return Err("client replay or Grafana frames differ".into());
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        }.await;
        if let Err(error) = result {
            row["error"] = json!(error.to_string());
            failures.push(error.to_string());
        } else {
            row["status"] = json!("matched");
        }
        report["cases"]
            .as_array_mut()
            .ok_or("report cases missing")?
            .push(row);
    }
    report["status"] = json!(if failures.is_empty() {
        "passed"
    } else {
        "failed"
    });
    if let Some(directory) = std::env::var_os("TEST_UNDECLARED_OUTPUTS_DIR") {
        std::fs::write(
            std::path::PathBuf::from(directory).join("grafana-loki-client-replay.json"),
            serde_json::to_vec_pretty(&report)?,
        )?;
    }
    if !failures.is_empty() {
        return Err(format!("Grafana client replay: {failures:?}").into());
    }
    Ok(())
}

#[test]
fn replay_comparator_rejects_wrong_values_order_labels_and_empty_frames() -> TestResult {
    let timeline = Timeline {
        base: 1_000_000_000,
        start: 0,
        end: 200_000_000_000,
    };
    let case = &CASES[0];
    let expected = expected_logs(case, &timeline);
    check_answer(case, &timeline, &expected)?;
    for field in ["stream", "values"] {
        let mut wrong = expected.clone();
        wrong["result"][0][field] = json!([]);
        assert!(check_answer(case, &timeline, &wrong).is_err());
    }
    let mut reversed = expected.clone();
    reversed["result"][1]["values"]
        .as_array_mut()
        .ok_or("info entries missing")?
        .reverse();
    assert!(check_answer(case, &timeline, &reversed).is_err());
    assert!(frames(&json!({"results":{"A":{"frames":[]}}})).is_err());
    let metric = json!({"resultType":"vector","result":[{"metric":{},"values":[[200,"3"]]}]});
    check_answer(&CASES[4], &timeline, &metric)?;
    let mut wrong = metric;
    wrong["result"][0]["values"][0][1] = json!("4");
    assert!(check_answer(&CASES[4], &timeline, &wrong).is_err());
    Ok(())
}

#[test]
fn grafana_frame_ledger_rejects_equal_but_wrong_client_values() -> TestResult {
    let timeline = Timeline {
        base: 1_000_000_000,
        start: 0,
        end: 200_000_000_000,
    };
    let response = json!({"results":{"A":{"frames":[{
        "schema":{"fields":[{"name":"Time","type":"time"},{"name":"Line","type":"string"}]},
        "data":{"values":[[9000,29000,49000],["INFO connection established","ERROR deadlock detected on shard 3","INFO vacuum complete"]]}
    }]}}});
    check_frames(&CASES[0], &timeline, &response)?;
    for column in 0..2 {
        let mut wrong = response.clone();
        wrong["results"]["A"]["frames"][0]["data"]["values"][column][0] = json!("wrong");
        assert!(check_frames(&CASES[0], &timeline, &wrong).is_err());
    }
    Ok(())
}

#[test]
fn frame_comparison_preserves_entry_order_within_each_unordered_stream() -> TestResult {
    let frame = json!({"schema":{"fields":[{"name":"labels"},{"name":"Line"}]},
        "data":{"values":[[{"app":"b"},{"app":"a"},{"app":"a"}],["b1","a1","a2"]]}});
    let reordered = json!({"schema":frame["schema"],
        "data":{"values":[[{"app":"a"},{"app":"a"},{"app":"b"}],["a1","a2","b1"]]}});
    assert!(canonical_frame(&frame)? == canonical_frame(&reordered)?);
    let mut reversed = reordered.clone();
    reversed["data"]["values"][1] = json!(["a2", "a1", "b1"]);
    assert!(canonical_frame(&frame)? != canonical_frame(&reversed)?);
    let mut missing = reordered;
    missing["data"]["values"][1] = json!(["a1"]);
    assert!(canonical_frame(&missing).is_err());
    Ok(())
}
