use assert2::assert;
use serde_json::{Value, json};

use super::{
    LOKI_CONFIG, OverridesProvider, TestResult, Timeline, mapped_base_url, normalize, push_json,
    start_krabka_with_overrides, start_loki_with_config, wait_for_ready, wait_for_seeded,
};
#[path = "experimental_queries.rs"]
mod fixture;

#[tokio::test]
#[ignore = "requires Docker"]
async fn loki_experimental_queries_match_native_and_independent_ledgers() -> TestResult {
    let config = LOKI_CONFIG.replace("limits_config:\n", "limits_config:\n  enable_multi_variant_queries: true\n  shard_aggregations: [approx_topk]\n  max_query_series: 2\n")
        .replace("querier:\n", "querier:\n  multi_tenant_queries_enabled: true\n")
        + "\nfrontend:\n  encoding: protobuf\nruntime_config:\n  file: /etc/loki/experimental-overrides.yaml\n";
    let client = reqwest::Client::new();
    let loki = start_loki_with_config(&config, fixture::LOKI_OVERRIDES).await?;
    let loki_url = mapped_base_url(&loki, super::LOKI_PORT).await?;
    let krabka =
        start_krabka_with_overrides(OverridesProvider::from_yaml(fixture::LIMITS_YAML)?).await?;
    wait_for_ready(&client, &loki_url).await?;
    let timeline = Timeline::new()?;
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
        for base in [&loki_url, &krabka.push_url] {
            push_json(&client, base, tenant, &fixture::seed(timeline.base_ns)).await?;
        }
        wait_for_seeded(&client, &loki_url, &timeline, tenant, "app", 4).await?;
    }
    let mut cases = Vec::new();
    let mut failures = Vec::new();
    for case in fixture::cases(timeline.base_ns) {
        let path = if case.range {
            "/loki/api/v1/query_range"
        } else {
            "/loki/api/v1/query"
        };
        let mut params = vec![("query", case.query.to_string())];
        if case.range {
            params.extend([
                ("start", timeline.at(40).to_string()),
                ("end", timeline.at(41).to_string()),
                ("step", "1".into()),
            ]);
        } else {
            params.push(("time", timeline.at(40).to_string()));
        }
        let oracle = exchange(&client, &loki_url, path, case.tenant, &params).await?;
        let candidate = exchange(&client, &krabka.query_url, path, case.tenant, &params).await?;
        let oracle_normal = normalized(&oracle);
        let candidate_normal = normalized(&candidate);
        let oracle_semantic = semantic(&oracle);
        let candidate_semantic = semantic(&candidate);
        let matched = oracle["status"] == case.expected_status
            && candidate["status"] == case.expected_status
            && oracle_normal == candidate_normal
            && oracle_semantic == case.expected
            && candidate_semantic == case.expected;
        if !matched {
            failures.push(format!(
                "{}: oracle={oracle_normal}; candidate={candidate_normal}; independent={}",
                case.id, case.expected
            ));
        }
        cases.push(json!({
            "id":case.id,"query":case.query,"request":{"method":"GET","path":path,"tenant":case.tenant,"params":params},
            "raw_oracle":oracle,"raw_candidate":candidate,
            "oracle":oracle_normal,"candidate":candidate_normal,
            "oracle_semantic":oracle_semantic,"candidate_semantic":candidate_semantic,
            "expected_status":case.expected_status,"independent_expected":case.expected,
            "expected_outcome":if case.expected_status == 200 { "query-result" } else { "query-rejection" },
            "classification":if !matched { "mismatch" } else if case.expected_status == 200 { "matched" } else { "paired-expected-error" },
            "status":if matched { if case.expected_status == 200 { "matched" } else { "paired-expected-error" } } else { "mismatch" },
        }));
    }
    let output = std::env::var_os("TEST_UNDECLARED_OUTPUTS_DIR").map_or_else(
        || std::path::PathBuf::from("../../target"),
        std::path::PathBuf::from,
    );
    std::fs::create_dir_all(&output)?;
    std::fs::write(
        output.join("loki-experimental-query-conformance.json"),
        serde_json::to_vec_pretty(&json!({
            "schema_version":1,"upstream_source":"7a40404f32b3e6464c9cfc6cc7dd75a40f3931da",
            "timeline_base_ns":timeline.base_ns,"planned":cases.len(),"cases":cases,
            "oracle_config":config,"oracle_overrides":fixture::LOKI_OVERRIDES,"candidate_overrides":fixture::LIMITS_YAML,
            "max_count_min_sketch_heap_size":10_000,
        }))?,
    )?;
    krabka.shutdown();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

async fn exchange(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    tenant: &str,
    params: &[(&str, String)],
) -> TestResult<Value> {
    let encoded = url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(params.iter().map(|(name, value)| (*name, value)))
        .finish();
    let response = client
        .get(format!("{base}{path}?{encoded}"))
        .header("X-Scope-OrgID", tenant)
        .send()
        .await?;
    let status = response.status().as_u16();
    let text = response.text().await?;
    let body = serde_json::from_str::<Value>(&text).unwrap_or_else(|_| Value::String(text.clone()));
    Ok(json!({"status":status,"body":body,"text":text}))
}

fn normalized(exchange: &Value) -> Value {
    if exchange["status"] == 200 {
        normalize(&exchange["body"])
    } else {
        json!({"status":exchange["status"],"error_code":"query-rejection","error":exchange["text"].as_str().unwrap().trim_end()})
    }
}

fn semantic(exchange: &Value) -> Value {
    if exchange["status"] == 200 {
        fixture::semantic_samples(&exchange["body"])
    } else {
        json!(exchange["text"].as_str().unwrap().trim_end())
    }
}
