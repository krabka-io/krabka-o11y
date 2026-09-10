//! The ruler API, and the Prometheus rules and alerts endpoints over its rule store.

mod support;

use assert2::{assert, check};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use krabka_blockstore::{LabelIndex, LogBlockIndex as BlockIndex, write_log_index_manifest};
use krabka_observability::{Role, ServiceDependencies, build_service_router, loki_router};
use serde_json::{Value, json};
use support::{assert_loki_error, fixture, json_body, test_service_config, text_body};
use tower::ServiceExt as _;

#[tokio::test]
async fn ruler_endpoints_match_empty_rule_and_alert_lists() {
    let state = fixture();
    let app = loki_router(state);

    let loki_rules_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/rules")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(loki_rules_response.status() == StatusCode::BAD_REQUEST);
    let content_type = loki_rules_response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let body = text_body(loki_rules_response).await;
    assert!(content_type.starts_with("text/plain"));
    assert!(
        body == "unable to read rule dir /loki/rules/fake: open /loki/rules/fake: no such file or directory\n"
    );

    let prometheus_rules_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/rules")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(prometheus_rules_response.status() == StatusCode::OK);
    assert!(
        json_body(prometheus_rules_response).await
            == json!({
                "status": "success",
                "data": {
                    "groups": []
                },
                "errorType": "",
                "error": ""
            })
    );

    let api_prom_rules_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/prom/rules")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(api_prom_rules_response.status() == StatusCode::BAD_REQUEST);
    let content_type = api_prom_rules_response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let body = text_body(api_prom_rules_response).await;
    assert!(content_type.starts_with("text/plain"));
    assert!(
        body == "unable to read rule dir /loki/rules/fake: open /loki/rules/fake: no such file or directory\n"
    );

    let prometheus_alerts_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/alerts")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(prometheus_alerts_response.status() == StatusCode::OK);
    assert!(
        json_body(prometheus_alerts_response).await
            == json!({
                "status": "success",
                "data": {
                    "alerts": []
                },
                "errorType": "",
                "error": ""
            })
    );

    let api_prom_alerts_response = app
        .oneshot(
            Request::builder()
                .uri("/api/prom/alerts")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(api_prom_alerts_response.status() == StatusCode::NOT_FOUND);
    let content_type = api_prom_alerts_response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let body = text_body(api_prom_alerts_response).await;
    assert!(content_type.starts_with("text/plain"));
    assert!(body == "404 page not found\n");
}

#[tokio::test]
async fn ruler_rule_group_read_endpoints_return_loki_not_found_errors() {
    let state = fixture();
    let app = loki_router(state);

    for uri in ["/loki/api/v1/rules/default", "/api/prom/rules/default"] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert!(response.status() == StatusCode::BAD_REQUEST);
        let body = text_body(response).await;
        assert!(
            body == "error parsing /loki/rules/fake/default: /loki/rules/fake/default: open /loki/rules/fake/default: no such file or directory\n"
        );
    }

    for uri in [
        "/loki/api/v1/rules/default/api-errors",
        "/api/prom/rules/default/api-errors",
    ] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert!(response.status() == StatusCode::BAD_REQUEST);
        let body = text_body(response).await;
        assert!(body == "GetRuleGroup unsupported in rule local store\n");
    }
}

#[tokio::test]
async fn ruler_rule_group_endpoint_stores_and_returns_yaml_rule_groups() {
    let state = fixture();
    let app = loki_router(state);
    let rule_group = "\
name: api-errors
interval: 1m
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [5m]) > 0
    for: 2m
    labels:
      severity: page
    annotations:
      summary: API errors detected
";

    let create_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/rules/default")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/yaml")
                .body(Body::from(rule_group))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(create_response.status() == StatusCode::ACCEPTED);
    assert!(
        json_body(create_response).await
            == json!({
                "status": "success"
            })
    );

    let group_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/rules/default/api-errors")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(group_response.status() == StatusCode::OK);
    let group_body = text_body(group_response).await;
    for needle in [
        "name: api-errors\n",
        "alert: ApiErrors\n",
        "severity: page\n",
    ] {
        check!(group_body.contains(needle));
    }

    let namespace_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/rules/default")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(namespace_response.status() == StatusCode::OK);
    let namespace_body = text_body(namespace_response).await;
    assert!(namespace_body.contains("- name: api-errors\n"));
    assert!(namespace_body.contains("alert: ApiErrors\n"));

    let all_rules_response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/rules")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(all_rules_response.status() == StatusCode::OK);
    let all_rules_body = text_body(all_rules_response).await;
    for needle in ["default:\n", "- name: api-errors\n", "alert: ApiErrors\n"] {
        check!(all_rules_body.contains(needle));
    }
}

#[tokio::test]
async fn ruler_rule_group_endpoint_rejects_invalid_rule_shapes() {
    let state = fixture();
    let app = loki_router(state);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/rules/default")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/yaml")
                .body(Body::from(
                    "\
name: bad-rules
rules:
  - alert: ApiErrors
    record: job:api_errors:rate5m
    expr: count_over_time({app=\"api\"} |= \"error\" [30ns]) > 0
",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(text_body(response).await == "unable to decoded rule group\n");

    let namespace_response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/rules/default")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(namespace_response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(namespace_response).await
            == "error parsing /loki/rules/tenant-a/default: /loki/rules/tenant-a/default: open /loki/rules/tenant-a/default: no such file or directory\n"
    );
}

#[tokio::test]
async fn ruler_rule_groups_persist_across_service_rebuilds() {
    let dir = tempfile::tempdir().unwrap().keep();
    write_log_index_manifest(&dir, &LabelIndex::default(), &BlockIndex::default()).unwrap();
    let config = test_service_config(Role::Querier, dir);

    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();
    post_loki_rule_group_for_test(
        &app,
        "default",
        "\
name: api-errors
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [30ns]) > 0
",
    )
    .await;

    let rebuilt_app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();
    let response = rebuilt_app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/rules/default")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = text_body(response).await;
    assert!(body.contains("- name: api-errors\n"));
    assert!(body.contains("alert: ApiErrors\n"));
}

#[tokio::test]
async fn prometheus_rules_endpoint_lists_stored_loki_rule_groups() {
    let state = fixture();
    let app = loki_router(state);
    let rule_group = "\
name: api-errors
interval: 1m
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [5m]) > 0
    for: 2m
    labels:
      severity: page
    annotations:
      summary: API errors detected
  - record: job:api_errors:rate5m
    expr: sum(rate({app=\"api\"} |= \"error\" [5m]))
    labels:
      job: api
";

    let create_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/rules/default")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/yaml")
                .body(Body::from(rule_group))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(create_response.status() == StatusCode::ACCEPTED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/rules")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "groups": [
                        {
                            "name": "api-errors",
                            "file": "default",
                            "interval": 60,
                            "limit": 0,
                            "rules": [
                                {
                                    "type": "alerting",
                                    "name": "ApiErrors",
                                    "query": "count_over_time({app=\"api\"} |= \"error\" [5m]) > 0",
                                    "duration": 120,
                                    "labels": {
                                        "severity": "page"
                                    },
                                    "annotations": {
                                        "summary": "API errors detected"
                                    },
                                    "alerts": [],
                                    "health": "ok"
                                },
                                {
                                    "type": "recording",
                                    "name": "job:api_errors:rate5m",
                                    "query": "sum(rate({app=\"api\"} |= \"error\" [5m]))",
                                    "labels": {
                                        "job": "api"
                                    },
                                    "health": "ok"
                                }
                            ]
                        }
                    ]
                },
                "errorType": "",
                "error": ""
            })
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/prom/rules")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = text_body(response).await;
    for needle in ["default:", "- name: api-errors\n", "alert: ApiErrors\n"] {
        check!(body.contains(needle));
    }
}

async fn post_loki_rule_group_for_test(app: &axum::Router, namespace: &str, rule_group: &str) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/loki/api/v1/rules/{namespace}"))
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/yaml")
                .body(Body::from(rule_group.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::ACCEPTED);
}

async fn prometheus_rules_body_for_test(app: &axum::Router, uri: &str) -> Value {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::OK);
    json_body(response).await
}

#[tokio::test]
async fn prometheus_rules_endpoint_filters_stored_loki_rule_groups() {
    let state = fixture();
    let app = loki_router(state);

    for (namespace, rule_group) in [
        (
            "default",
            "\
name: api-errors
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [5m]) > 0
  - record: job:api_errors:rate5m
    expr: sum(rate({app=\"api\"} |= \"error\" [5m]))
",
        ),
        (
            "jobs",
            "\
name: worker-errors
rules:
  - alert: WorkerErrors
    expr: count_over_time({app=\"worker\"} |= \"error\" [5m]) > 0
",
        ),
    ] {
        post_loki_rule_group_for_test(&app, namespace, rule_group).await;
    }

    let record_body =
        prometheus_rules_body_for_test(&app, "/prometheus/api/v1/rules?type=record").await;
    check!(record_body["data"]["groups"].as_array().unwrap().len() == 1);
    check!(record_body["data"]["groups"][0]["name"] == "api-errors");
    check!(
        record_body["data"]["groups"][0]["rules"]
            .as_array()
            .unwrap()
            .len()
            == 1
    );
    check!(record_body["data"]["groups"][0]["rules"][0]["type"] == "recording");
    check!(record_body["data"]["groups"][0]["rules"][0]["name"] == "job:api_errors:rate5m");

    let alert_name_body =
        prometheus_rules_body_for_test(&app, "/prometheus/api/v1/rules?rule_name[]=WorkerErrors")
            .await;
    check!(alert_name_body["data"]["groups"].as_array().unwrap().len() == 1);
    check!(alert_name_body["data"]["groups"][0]["file"] == "jobs");
    check!(
        alert_name_body["data"]["groups"][0]["rules"]
            .as_array()
            .unwrap()
            .len()
            == 1
    );
    check!(alert_name_body["data"]["groups"][0]["rules"][0]["name"] == "WorkerErrors");

    let group_file_body = prometheus_rules_body_for_test(
        &app,
        "/prometheus/api/v1/rules?rule_group[]=api-errors&file[]=default",
    )
    .await;
    check!(group_file_body["data"]["groups"].as_array().unwrap().len() == 1);
    check!(group_file_body["data"]["groups"][0]["name"] == "api-errors");
    check!(group_file_body["data"]["groups"][0]["file"] == "default");
    check!(
        group_file_body["data"]["groups"][0]["rules"]
            .as_array()
            .unwrap()
            .len()
            == 2
    );
}

#[tokio::test]
async fn prometheus_alerts_endpoint_lists_firing_loki_rule_alerts() {
    let state = fixture();
    let app = loki_router(state);
    post_loki_rule_group_for_test(
        &app,
        "default",
        "\
name: api-errors
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [30ns]) > 0
    labels:
      severity: page
    annotations:
      summary: API errors detected
",
    )
    .await;

    let alerts_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/alerts?time=19")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(alerts_response.status() == StatusCode::OK);
    assert!(
        json_body(alerts_response).await
            == json!({
                "status": "success",
                "data": {
                    "alerts": [
                        {
                            "activeAt": "1970-01-01T00:00:00.000000019Z",
                            "annotations": {
                                "summary": "API errors detected"
                            },
                            "labels": {
                                "alertname": "ApiErrors",
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod",
                                "severity": "page"
                            },
                            "state": "firing",
                            "value": "1"
                        }
                    ]
                },
                "errorType": "",
                "error": ""
            })
    );

    let rules_body =
        prometheus_rules_body_for_test(&app, "/prometheus/api/v1/rules?time=19&type=alert").await;
    assert!(rules_body["data"]["groups"][0]["rules"][0]["alerts"][0]["state"] == "firing");
    assert!(
        rules_body["data"]["groups"][0]["rules"][0]["alerts"][0]["labels"]["alertname"]
            == "ApiErrors"
    );
}

#[tokio::test]
async fn prometheus_alerts_endpoint_expands_loki_rule_label_and_annotation_templates() {
    let state = fixture();
    let app = loki_router(state);
    post_loki_rule_group_for_test(
        &app,
        "default",
        "\
name: api-errors
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [30ns]) > 0
    labels:
      route: '{{ $labels.app }}-{{ $labels.env }}'
    annotations:
      summary: 'service={{ $labels.app }} value={{ $value }}'
",
    )
    .await;

    let alerts_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/alerts?time=19")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(alerts_response.status() == StatusCode::OK);
    let body = json_body(alerts_response).await;
    check!(body["data"]["alerts"].as_array().unwrap().len() == 1);
    check!(body["data"]["alerts"][0]["labels"]["route"] == "api-prod");
    check!(body["data"]["alerts"][0]["annotations"]["summary"] == "service=api value=1");
}

#[tokio::test]
async fn prometheus_alerts_endpoint_expands_compact_loki_rule_templates() {
    let state = fixture();
    let app = loki_router(state);
    post_loki_rule_group_for_test(
        &app,
        "default",
        "\
name: api-errors
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [30ns]) > 0
    labels:
      route: '{{$labels.app}}-{{$labels.env}}'
    annotations:
      summary: 'service={{$labels.app}} value={{$value}}'
",
    )
    .await;

    let alerts_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/alerts?time=19")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(alerts_response.status() == StatusCode::OK);
    let body = json_body(alerts_response).await;
    check!(body["data"]["alerts"].as_array().unwrap().len() == 1);
    check!(body["data"]["alerts"][0]["labels"]["route"] == "api-prod");
    check!(body["data"]["alerts"][0]["annotations"]["summary"] == "service=api value=1");
}

#[tokio::test]
async fn prometheus_alerts_endpoint_tracks_pending_alerts_until_for_duration_elapses() {
    let state = fixture();
    let app = loki_router(state);
    post_loki_rule_group_for_test(
        &app,
        "default",
        "\
name: api-errors
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [30ns]) > 0
    for: 20ns
    labels:
      severity: page
",
    )
    .await;

    let pending_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/alerts?time=19")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(pending_response.status() == StatusCode::OK);
    let pending_body = json_body(pending_response).await;
    check!(pending_body["data"]["alerts"].as_array().unwrap().len() == 1);
    check!(pending_body["data"]["alerts"][0]["state"] == "pending");
    check!(pending_body["data"]["alerts"][0]["activeAt"] == "1970-01-01T00:00:00.000000019Z");

    let firing_body =
        prometheus_rules_body_for_test(&app, "/prometheus/api/v1/rules?time=40&type=alert").await;
    assert!(firing_body["data"]["groups"][0]["rules"][0]["alerts"][0]["state"] == "firing");
    assert!(
        firing_body["data"]["groups"][0]["rules"][0]["alerts"][0]["activeAt"]
            == "1970-01-01T00:00:00.000000019Z"
    );
}

#[tokio::test]
async fn prometheus_alerts_endpoint_honors_keep_firing_for_after_condition_resolves() {
    let state = fixture();
    let app = loki_router(state);
    post_loki_rule_group_for_test(
        &app,
        "default",
        "\
name: api-errors
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [30ns]) > 0
    keep_firing_for: 50ns
",
    )
    .await;

    let firing_body =
        prometheus_rules_body_for_test(&app, "/prometheus/api/v1/rules?time=40&type=alert").await;
    assert!(firing_body["data"]["groups"][0]["rules"][0]["alerts"][0]["state"] == "firing");

    let retained_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/alerts?time=80")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(retained_response.status() == StatusCode::OK);
    let retained_body = json_body(retained_response).await;
    check!(retained_body["data"]["alerts"].as_array().unwrap().len() == 1);
    check!(retained_body["data"]["alerts"][0]["state"] == "firing");
    check!(retained_body["data"]["alerts"][0]["activeAt"] == "1970-01-01T00:00:00.00000004Z");

    let resolved_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/alerts?time=100")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(resolved_response.status() == StatusCode::OK);
    let resolved_body = json_body(resolved_response).await;
    assert!(resolved_body["data"]["alerts"] == json!([]));
}

#[tokio::test]
async fn ruler_rule_group_recreate_resets_alert_for_duration_state() {
    let state = fixture();
    let app = loki_router(state);
    let rule_group = "\
name: api-errors
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [30ns]) > 0
    for: 20ns
";
    post_loki_rule_group_for_test(&app, "default", rule_group).await;

    let first_body =
        prometheus_rules_body_for_test(&app, "/prometheus/api/v1/rules?time=19&type=alert").await;
    assert!(first_body["data"]["groups"][0]["rules"][0]["alerts"][0]["state"] == "pending");

    let delete_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/loki/api/v1/rules/default/api-errors")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(delete_response.status() == StatusCode::ACCEPTED);

    post_loki_rule_group_for_test(&app, "default", rule_group).await;
    let recreated_body =
        prometheus_rules_body_for_test(&app, "/prometheus/api/v1/rules?time=40&type=alert").await;
    assert!(recreated_body["data"]["groups"][0]["rules"][0]["alerts"][0]["state"] == "pending");
    assert!(
        recreated_body["data"]["groups"][0]["rules"][0]["alerts"][0]["activeAt"]
            == "1970-01-01T00:00:00.00000004Z"
    );
}

#[tokio::test]
async fn prometheus_rules_endpoint_excludes_active_alerts_when_requested() {
    let state = fixture();
    let app = loki_router(state);
    post_loki_rule_group_for_test(
        &app,
        "default",
        "\
name: api-errors
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [30ns]) > 0
",
    )
    .await;

    let body = prometheus_rules_body_for_test(
        &app,
        "/prometheus/api/v1/rules?time=19&type=alert&exclude_alerts=true",
    )
    .await;

    check!(body["data"]["groups"].as_array().unwrap().len() == 1);
    check!(body["data"]["groups"][0]["rules"][0]["name"] == "ApiErrors");
    check!(body["data"]["groups"][0]["rules"][0]["alerts"] == json!([]));
}

#[tokio::test]
async fn prometheus_rules_endpoint_filters_rules_by_configured_labels() {
    let state = fixture();
    let app = loki_router(state);
    post_loki_rule_group_for_test(
        &app,
        "default",
        "\
name: api-rules
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [30ns]) > 0
    labels:
      severity: page
      team: api
  - alert: WorkerErrors
    expr: count_over_time({app=\"worker\"} |= \"error\" [30ns]) > 0
    labels:
      team: batch
",
    )
    .await;

    let body = prometheus_rules_body_for_test(
        &app,
        "/prometheus/api/v1/rules?exclude_alerts=true&match[]=%7Bseverity%3D%22page%22%7D",
    )
    .await;

    check!(body["data"]["groups"].as_array().unwrap().len() == 1);
    check!(body["data"]["groups"][0]["rules"].as_array().unwrap().len() == 1);
    check!(body["data"]["groups"][0]["rules"][0]["name"] == "ApiErrors");
    check!(body["data"]["groups"][0]["rules"][0]["labels"]["severity"] == "page");
}

#[tokio::test]
async fn prometheus_rules_endpoint_paginates_rule_groups() {
    let state = fixture();
    let app = loki_router(state);

    for (namespace, rule_group) in [
        (
            "alpha",
            "\
name: api-rules
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [30ns]) > 0
",
        ),
        (
            "bravo",
            "\
name: worker-rules
rules:
  - alert: WorkerErrors
    expr: count_over_time({app=\"worker\"} |= \"error\" [30ns]) > 0
",
        ),
        (
            "charlie",
            "\
name: search-rules
rules:
  - alert: SearchErrors
    expr: count_over_time({app=\"search\"} |= \"error\" [30ns]) > 0
",
        ),
    ] {
        post_loki_rule_group_for_test(&app, namespace, rule_group).await;
    }

    let first_page = prometheus_rules_body_for_test(
        &app,
        "/prometheus/api/v1/rules?exclude_alerts=true&group_limit=2",
    )
    .await;
    check!(first_page["data"]["groups"].as_array().unwrap().len() == 2);
    check!(first_page["data"]["groups"][0]["name"] == "api-rules");
    check!(first_page["data"]["groups"][1]["name"] == "worker-rules");
    let token = first_page["data"]["groupNextToken"]
        .as_str()
        .expect("expected next page token");

    let second_page = prometheus_rules_body_for_test(
        &app,
        &format!(
            "/prometheus/api/v1/rules?exclude_alerts=true&group_limit=2&group_next_token={token}"
        ),
    )
    .await;
    check!(second_page["data"]["groups"].as_array().unwrap().len() == 1);
    check!(second_page["data"]["groups"][0]["name"] == "search-rules");
    check!(second_page["data"].get("groupNextToken").is_none());
}

#[tokio::test]
async fn prometheus_rules_endpoint_rejects_stale_group_next_token() {
    let state = fixture();
    let app = loki_router(state);
    post_loki_rule_group_for_test(
        &app,
        "alpha",
        "\
name: api-rules
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [30ns]) > 0
",
    )
    .await;
    post_loki_rule_group_for_test(
        &app,
        "bravo",
        "\
name: worker-rules
rules:
  - alert: WorkerErrors
    expr: count_over_time({app=\"worker\"} |= \"error\" [30ns]) > 0
",
    )
    .await;

    let first_page = prometheus_rules_body_for_test(
        &app,
        "/prometheus/api/v1/rules?exclude_alerts=true&group_limit=1",
    )
    .await;
    let token = first_page["data"]["groupNextToken"]
        .as_str()
        .expect("expected next page token")
        .to_string();

    let delete_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/loki/api/v1/rules/alpha")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(delete_response.status() == StatusCode::ACCEPTED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/prometheus/api/v1/rules?exclude_alerts=true&group_limit=1&group_next_token={token}"
                ))
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(response).await, "bad_data", "group_next_token");
}

#[tokio::test]
async fn prometheus_rules_endpoint_rejects_group_next_token_without_matching_rule_store() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/rules?group_limit=1&group_next_token=stale")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(response).await, "bad_data", "group_next_token");
}

#[tokio::test]
async fn ruler_rule_group_delete_endpoint_removes_only_the_named_group() {
    let state = fixture();
    let app = loki_router(state);

    for rule_group in [
        "\
name: api-errors
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [5m]) > 0
",
        "\
name: worker-errors
rules:
  - alert: WorkerErrors
    expr: count_over_time({app=\"worker\"} |= \"error\" [5m]) > 0
",
    ] {
        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/loki/api/v1/rules/default")
                    .header("X-Scope-OrgID", "tenant-a")
                    .header("content-type", "application/yaml")
                    .body(Body::from(rule_group))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(create_response.status() == StatusCode::ACCEPTED);
    }

    let delete_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/loki/api/v1/rules/default/api-errors")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(delete_response.status() == StatusCode::ACCEPTED);

    let deleted_group_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/rules/default/api-errors")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(deleted_group_response.status() == StatusCode::NOT_FOUND);
    assert!(text_body(deleted_group_response).await == "group does not exist\n");

    let namespace_response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/rules/default")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(namespace_response.status() == StatusCode::OK);
    let namespace_body = text_body(namespace_response).await;
    check!(!namespace_body.contains("api-errors"));
    check!(namespace_body.contains("worker-errors"));
    check!(namespace_body.contains("WorkerErrors"));
}

#[tokio::test]
async fn ruler_namespace_delete_endpoint_removes_only_that_namespace() {
    let state = fixture();
    let app = loki_router(state);

    for (namespace, rule_group) in [
        (
            "default",
            "\
name: api-errors
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [5m]) > 0
",
        ),
        (
            "jobs",
            "\
name: worker-errors
rules:
  - alert: WorkerErrors
    expr: count_over_time({app=\"worker\"} |= \"error\" [5m]) > 0
",
        ),
    ] {
        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/loki/api/v1/rules/{namespace}"))
                    .header("X-Scope-OrgID", "tenant-a")
                    .header("content-type", "application/yaml")
                    .body(Body::from(rule_group))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(create_response.status() == StatusCode::ACCEPTED);
    }

    let delete_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/loki/api/v1/rules/default")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(delete_response.status() == StatusCode::ACCEPTED);

    let deleted_namespace_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/rules/default")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(deleted_namespace_response.status() == StatusCode::NOT_FOUND);
    assert!(text_body(deleted_namespace_response).await == "no rule groups found\n");

    let other_namespace_response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/rules/jobs/worker-errors")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(other_namespace_response.status() == StatusCode::OK);
    let other_namespace_body = text_body(other_namespace_response).await;
    assert!(other_namespace_body.contains("worker-errors"));
    assert!(other_namespace_body.contains("WorkerErrors"));
}

#[tokio::test]
async fn ruler_rule_group_delete_endpoint_removes_empty_namespace() {
    let state = fixture();
    let app = loki_router(state);
    let rule_group = "\
name: api-errors
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [5m]) > 0
";

    let create_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/rules/default")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/yaml")
                .body(Body::from(rule_group))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(create_response.status() == StatusCode::ACCEPTED);

    let delete_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/loki/api/v1/rules/default/api-errors")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(delete_response.status() == StatusCode::ACCEPTED);

    let namespace_response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/rules/default")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(namespace_response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(namespace_response).await
            == "error parsing /loki/rules/tenant-a/default: /loki/rules/tenant-a/default: open /loki/rules/tenant-a/default: no such file or directory\n"
    );
}

#[tokio::test]
async fn ruler_ring_endpoint_returns_loki_status_page() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/ruler/ring")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let body = text_body(response).await;
    assert!(content_type.starts_with("text/html"));
    assert!(body.contains("Cortex Ruler Status"));
}
