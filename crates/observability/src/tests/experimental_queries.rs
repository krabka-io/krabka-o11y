use assert2::assert;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::Request,
};
use clap::Parser as _;
use krabka_units::convert::TimeExt as _;
use serde_json::Value;
use tower::ServiceExt as _;

use super::prelude::{
    BlockIndex, BufferedLogHotTail, LabelIndex, Labels, Limits, LokiTypedPushRequest,
    OverridesProvider, QuerierState, ServiceConfig, TenantId, Time, WalLogRecord,
    limits_for_config, loki_router, normalize_loki_push,
};
#[path = "../../tests/support/experimental_queries.rs"]
mod fixture;

/// A `GET` of `uri`, sent as `tenant`.
struct TenantGet<'a> {
    uri: String,
    tenant: &'a str,
}

/// What the router answered a [`TenantGet`] with.
struct RouterReply {
    status: u16,
    body: axum::body::Bytes,
}

impl TenantGet<'_> {
    async fn send(self, router: &Router) -> RouterReply {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(self.uri)
                    .header("X-Scope-OrgID", self.tenant)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        RouterReply {
            status: response.status().as_u16(),
            body: to_bytes(response.into_body(), 1_000_000).await.unwrap(),
        }
    }
}

#[test]
fn experimental_flags_are_default_disabled_and_tenant_lists_replace_inherited_lists() {
    let defaults = ServiceConfig::default();
    assert!(!defaults.enable_multi_variant_queries);
    assert!(defaults.shard_aggregations.is_empty());
    assert!(defaults.max_count_min_sketch_heap_size == 10_000);
    assert!(limits_for_config(&defaults) == Limits::default());
    let config = ServiceConfig::try_parse_from([
        "krabka-observability",
        "--target=querier",
        "--enable-multi-variant-queries",
        "--shard-aggregations=approx_topk,sum",
        "--max-count-min-sketch-heap-size=7",
    ])
    .unwrap();
    let limits = limits_for_config(&config);
    assert!(limits.enable_multi_variant_queries);
    assert!(limits.shard_aggregations == ["approx_topk", "sum"]);
    let state = QuerierState::new(".", LabelIndex::default(), BlockIndex::default())
        .with_runtime_policy(&config);
    assert!(state.max_count_min_sketch_heap_size == 7);
    assert!(OverridesProvider::from_yaml(fixture::LOKI_OVERRIDES).is_ok());
    let provider = OverridesProvider::from_yaml(fixture::LIMITS_YAML).unwrap();
    let disabled = provider
        .for_tenant(&TenantId::new("experimental-disabled").unwrap())
        .clone();
    assert!(!disabled.enable_multi_variant_queries && disabled.shard_aggregations.is_empty());
    let ordinary = provider.for_tenant(&TenantId::new("unlisted").unwrap());
    assert!(
        ordinary.enable_multi_variant_queries && ordinary.shard_aggregations == ["approx_topk"]
    );
}

#[tokio::test]
async fn public_experimental_queries_follow_independent_input_and_output_ledgers() {
    let base = 1_700_000_000_000_000_000;
    let router = seeded_router(base);
    let mut cases = fixture::cases(base);
    let absence = cases
        .iter()
        .find(|case| case.id == "variants/absent-instant-original-synthetic-labels")
        .unwrap()
        .expected
        .clone();
    cases.extend([
        fixture::Case {
            id: "federated-absence-all-missing",
            query: r#"variants(absent_over_time({app="original"}[1m])) of ({app="missing"}[1m])"#,
            tenant: "experimental|experimental-empty",
            range: false,
            expected_status: 200,
            expected: absence,
        },
        fixture::Case {
            id: "federated-absence-one-present",
            query: r#"variants(absent_over_time({app="original"}[1m])) of ({app="api"}[1m])"#,
            tenant: "experimental|experimental-empty",
            range: false,
            expected_status: 200,
            expected: serde_json::json!({"samples":[],"warnings":[]}),
        },
    ]);
    for case in cases {
        let mut params = url::form_urlencoded::Serializer::new(String::new());
        params.append_pair("query", case.query);
        if case.range {
            params
                .append_pair("start", &(base + 40_000_000_000).to_string())
                .append_pair("end", &(base + 41_000_000_000).to_string())
                .append_pair("step", "1");
        } else {
            params.append_pair("time", &(base + 40_000_000_000).to_string());
        }
        let path = if case.range { "query_range" } else { "query" };
        let response = TenantGet {
            uri: format!("/loki/api/v1/{path}?{}", params.finish()),
            tenant: case.tenant,
        }
        .send(&router)
        .await;
        let status = response.status;
        let bytes = response.body;
        assert!(
            status == case.expected_status,
            "{}: {}",
            case.id,
            String::from_utf8_lossy(&bytes)
        );
        let actual = if status == 200 {
            fixture::semantic_samples(&serde_json::from_slice::<Value>(&bytes).unwrap())
        } else {
            Value::String(
                String::from_utf8(bytes.to_vec())
                    .unwrap()
                    .trim_end()
                    .to_string(),
            )
        };
        assert!(actual == case.expected, "{}", case.id);
    }
}

fn seeded_router(base: i64) -> Router {
    let seed = fixture::seed(base);
    let tail = BufferedLogHotTail::default();
    let mut records = Vec::new();
    let provider = OverridesProvider::from_yaml(fixture::LIMITS_YAML).unwrap();
    for tenant in [
        "experimental",
        "experimental-one",
        "experimental-zero",
        "experimental-disabled",
        "experimental-disabled2",
        "experimental-discovery-disabled",
        "experimental-discovery-custom",
        "experimental-discovery-deep",
    ] {
        let tenant = TenantId::new(tenant).unwrap();
        let limits = Limits {
            reject_old_samples_max_age: Time::ZERO,
            ..provider.for_tenant(&tenant).clone()
        };
        let payload = serde_json::from_value::<LokiTypedPushRequest>(seed.clone()).unwrap();
        records.extend(normalize_loki_push(&tenant, payload, &limits).unwrap());
    }
    tail.append_records(records);
    let state = QuerierState::new(".", LabelIndex::default(), BlockIndex::default())
        .with_hot_tail(tail, i64::MIN)
        .with_limits_overrides(OverridesProvider::from_yaml(fixture::LIMITS_YAML).unwrap());
    loki_router(state)
}

#[tokio::test]
async fn variants_reject_invalid_and_nested_grammar_on_public_http() {
    let base = 1_700_000_000_000_000_000;
    let router = seeded_router(base);
    // Syntax and top-level grammar fail before execution, even when enabled.
    for query in [
        "variants() of ({app=\"api\"}[1m])",
        "variants(count_over_time({app=\"api\"}[1m]))",
        "sum(variants(count_over_time({app=\"api\"}[1m])) of ({app=\"api\"}[1m]))",
    ] {
        let encoded = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("query", query)
            .append_pair("time", &(base + 40_000_000_000).to_string())
            .finish();
        let response = TenantGet {
            uri: format!("/loki/api/v1/query?{encoded}"),
            tenant: "experimental",
        }
        .send(&router)
        .await;
        assert!(response.status == 400);
    }
}

#[tokio::test]
async fn experimental_queries_are_rejected_by_default_on_public_http() {
    let router = loki_router(QuerierState::new(
        ".",
        LabelIndex::default(),
        BlockIndex::default(),
    ));
    for (query, expected, status) in [
        (
            r#"variants(count_over_time({app="api"}[1m])) of ({app="api"}[1m])"#,
            "multi variant queries are disabled for this instance",
            400,
        ),
        (
            r#"approx_topk(1,count_over_time({app="api"}[1m]))"#,
            "approx_topk is not enabled. See -limits.shard_aggregations",
            500,
        ),
    ] {
        let params = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("query", query)
            .finish();
        let response = TenantGet {
            uri: format!("/loki/api/v1/query?{params}"),
            tenant: "unlisted",
        }
        .send(&router)
        .await;
        assert!(response.status == status);
        let bytes = response.body;
        assert!(String::from_utf8(bytes.to_vec()).unwrap().trim_end() == expected);
    }
}

#[tokio::test]
async fn unlabelled_approximation_rejects_before_flags_or_query_kind() {
    let base = 1_700_000_000_000_000_000;
    let router = seeded_router(base);
    for tenant in [
        "experimental",
        "experimental-disabled",
        "experimental|experimental-disabled",
    ] {
        for path in ["query", "query_range"] {
            let params = url::form_urlencoded::Serializer::new(String::new())
                .append_pair("query", "approx_topk(1,vector(3))")
                .append_pair("time", &(base + 40_000_000_000).to_string())
                .append_pair("start", &(base + 40_000_000_000).to_string())
                .append_pair("end", &(base + 41_000_000_000).to_string())
                .append_pair("step", "1")
                .finish();
            let response = TenantGet {
                uri: format!("/loki/api/v1/{path}?{params}"),
                tenant,
            }
            .send(&router)
            .await;
            assert!(response.status == 400);
            let bytes = response.body;
            assert!(
                String::from_utf8(bytes.to_vec()).unwrap().trim_end()
                    == fixture::UNLABELLED_APPROX_ERROR
            );
        }
    }
}

#[tokio::test]
async fn common_variant_metadata_is_reentered_before_filtering_and_grouping() {
    let base = 1_700_000_000_000_000_000;
    let tail = BufferedLogHotTail::default();
    tail.append_records(Vec::from(["blue", "red"].map(|value| WalLogRecord {
        tenant: "metadata".into(),
        labels: Labels::from([("app".into(), "api".into())]),
        timestamp_ns: base,
        line: "keep".into(),
        structured_metadata: Labels::from([("token".into(), value.into())]),
        position: None,
    })));
    let router = loki_router(
        QuerierState::new(".", LabelIndex::default(), BlockIndex::default())
            .with_hot_tail(tail, i64::MIN)
            .with_limits(Limits {
                enable_multi_variant_queries: true,
                ..Limits::default()
            }),
    );
    for (query, expected) in [
        (
            r#"variants(sum by(token_extracted)(count_over_time({app="ignored"}[1m]))) of ({app="api"}[1m])"#,
            serde_json::json!([[{"__variant__":"0","token_extracted":"blue"},"1"],[{"__variant__":"0","token_extracted":"red"},"1"]]),
        ),
        (
            r#"variants(sum by(token_extracted)(count_over_time({app="ignored"}[1m]))) of ({app="api"} | drop token [1m])"#,
            serde_json::json!([[{"__variant__":"0"},"2"]]),
        ),
        (
            r#"variants(sum by(token_extracted)(count_over_time({app="ignored"}[1m]))) of ({app="api"} | label_format token="{{.token}}" [1m])"#,
            serde_json::json!([[{"__variant__":"0"},"2"]]),
        ),
        (
            r#"variants(sum by(token_extracted)(count_over_time({app="ignored"} | token_extracted="blue" [1m]))) of ({app="api"}[1m])"#,
            serde_json::json!([[{"__variant__":"0","token_extracted":"blue"},"1"]]),
        ),
        (
            r#"sum by(token_extracted)(count_over_time({app="api"}[1m]))"#,
            serde_json::json!([[{}, "2"]]),
        ),
    ] {
        let params = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("query", query)
            .append_pair("time", &(base + 40_000_000_000).to_string())
            .finish();
        let response = TenantGet {
            uri: format!("/loki/api/v1/query?{params}"),
            tenant: "metadata",
        }
        .send(&router)
        .await;
        let status = response.status;
        let bytes = response.body;
        assert!(
            status == 200,
            "{query}: {}",
            String::from_utf8_lossy(&bytes)
        );
        let response: Value = serde_json::from_slice(&bytes).unwrap();
        let mut rows = response["data"]["result"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| serde_json::json!([row["metric"], row["value"][1]]))
            .collect::<Vec<_>>();
        rows.sort_by_key(ToString::to_string);
        assert!(serde_json::json!(rows) == expected, "{query}");
    }
}
