use num_traits::ToPrimitive;
use serde_json::{Value, json};

pub const LIMITS_YAML: &str = "defaults:\n  enable_multi_variant_queries: true\n  shard_aggregations: [approx_topk]\n  max_query_series: 2\noverrides:\n  experimental-disabled:\n    enable_multi_variant_queries: false\n    shard_aggregations: []\n  experimental-disabled2:\n    enable_multi_variant_queries: false\n    shard_aggregations: []\n  experimental-one:\n    max_query_series: 1\n  experimental-zero:\n    max_query_series: 0\n  experimental-discovery-disabled:\n    discover_log_levels: false\n  experimental-discovery-custom:\n    log_level_fields: [priority]\n    log_level_from_json_max_depth: 2\n  experimental-discovery-deep:\n    log_level_fields: [priority]\n    log_level_from_json_max_depth: 0\n";
pub const LOKI_OVERRIDES: &str = "overrides:\n  experimental-disabled:\n    enable_multi_variant_queries: false\n    shard_aggregations: []\n  experimental-disabled2:\n    enable_multi_variant_queries: false\n    shard_aggregations: []\n  experimental-one:\n    max_query_series: 1\n  experimental-zero:\n    max_query_series: 0\n  experimental-discovery-disabled:\n    discover_log_levels: false\n  experimental-discovery-custom:\n    log_level_fields: [priority]\n    log_level_from_json_max_depth: 2\n  experimental-discovery-deep:\n    log_level_fields: [priority]\n    log_level_from_json_max_depth: 0\n";
pub const UNLABELLED_APPROX_ERROR: &str = "parse error : queries require at least one regexp or equality matcher that does not have an empty-compatible value. For instance, app=~\".*\" does not meet this requirement, but app=~\".+\" will";

pub struct Case {
    pub id: &'static str,
    pub query: &'static str,
    pub tenant: &'static str,
    pub range: bool,
    pub expected_status: u16,
    pub expected: Value,
}

pub fn seed(base: i64) -> Value {
    json!({"streams":[
        {"stream":{"app":"api","service_name":"api"},"values":[
            [(base+10_000_000_000).to_string(),"keep a"],
            [(base+15_000_000_000).to_string(),"drop"],
            [(base+20_000_000_000).to_string(),"keep a"],
            [(base+25_000_000_000).to_string(),"keep a"]]},
        {"stream":{"app":"web","service_name":"web"},"values":[
            [(base+10_000_000_000).to_string(),"keep b"],
            [(base+30_000_000_000).to_string(),"keep b"]]},
        {"stream":{"app":"numeric","service_name":"numeric"},"values":[
            [(base+10_000_000_000).to_string(),"n=2"],
            [(base+20_000_000_000).to_string(),"n=5"]]}
        ,{"stream":{"app":"discovery","service_name":"discovery"},"values":[
            [(base+10_000_000_000).to_string(),"priority=DBG error=none"],
            [(base+20_000_000_000).to_string(),r#"{"outer":{"inner":{"priority":"INF"}}}"#]]}
    ]})
}

type LedgerRow = (&'static str, &'static str, &'static str);
type PositiveCase = (&'static str, &'static str, &'static [LedgerRow]);
const INSTANT_CASES: &[PositiveCase] = &[
    (
        "variants/absent-instant-original-synthetic-labels",
        r#"variants(absent_over_time({app="original"}[1m])) of ({app="missing"}[1m])"#,
        &[("", "original", "1")],
    ),
    (
        "variants/topk-by-ranks-each-app",
        r#"variants(topk by(app)(1,count_over_time({app="ignored"}[1m]))) of ({app=~"api|web"} |= "keep" [1m])"#,
        &[("0", "api", "3"), ("0", "web", "2")],
    ),
    (
        "variants/bottomk-by-ranks-each-app",
        r#"variants(bottomk by(app)(1,count_over_time({app="ignored"}[1m]))) of ({app=~"api|web"} |= "keep" [1m])"#,
        &[("0", "api", "3"), ("0", "web", "2")],
    ),
    (
        "variants/bare-selector-and-prefilter-ignored",
        r#"variants(count_over_time({app="ignored"} |= "absent" [1m]),bytes_over_time({app="ignored"}[1m])) of ({app=~"api|web"} |= "keep" [1m])"#,
        &[
            ("0", "api", "3"),
            ("0", "web", "2"),
            ("1", "api", "18"),
            ("1", "web", "12"),
        ],
    ),
    (
        "variants/vector-retains-own-pipeline",
        r#"variants(sum by (app)(count_over_time({app="ignored"} |= "a" [1m]))) of ({app=~"api|web"} |= "keep" [1m])"#,
        &[("0", "api", "3")],
    ),
    (
        "variants/by-retains-variant",
        r#"variants(sum by (app)(count_over_time({app="ignored"}[1m])),sum(count_over_time({app="ignored"}[1m]))) of ({app=~"api|web"} |= "keep" [1m])"#,
        &[("0", "api", "3"), ("0", "web", "2"), ("1", "", "5")],
    ),
    (
        "variants/without-instant-retains-unlabelled-vector",
        r#"variants(sum without(app,service_name,detected_level,detected_level_extracted)(count_over_time({app="ignored"}[1m]))) of ({app=~"api|web"} |= "keep" [1m])"#,
        &[("", "", "5")],
    ),
    (
        "variants/unwrap-common-parser",
        r#"variants(sum_over_time({app="ignored"} | json | unwrap n | __error__="" [1m])) of ({app="numeric"} | logfmt [1m])"#,
        &[("0", "numeric", "7")],
    ),
    (
        "variants/missing-common-selector",
        r#"variants(count_over_time({app="api"}[1m])) of ({app="missing"}[1m])"#,
        &[],
    ),
    (
        "variants/own-window",
        r#"variants(count_over_time({app="ignored"}[5s]),count_over_time({app="ignored"}[1m])) of ({app=~"api|web"} |= "keep" [1m])"#,
        &[("1", "api", "3"), ("1", "web", "2")],
    ),
    (
        "variants/common-window-bounds-reads",
        r#"variants(count_over_time({app="ignored"}[1m])) of ({app=~"api|web"} |= "keep" [5s])"#,
        &[],
    ),
    (
        "variants/common-offset-bounds-reads",
        r#"variants(count_over_time({app="ignored"}[1m])) of ({app=~"api|web"} |= "keep" [1m] offset 30s)"#,
        &[("0", "api", "1"), ("0", "web", "1")],
    ),
    (
        "variants/own-offset",
        r#"variants(count_over_time({app="ignored"}[1m] offset 20s)) of ({app=~"api|web"} |= "keep" [1m])"#,
        &[("0", "api", "2"), ("0", "web", "1")],
    ),
    (
        "variants/independent-series-budgets",
        r#"variants(count_over_time({app="ignored"}[1m]),count_over_time({app="ignored"}[1m])) of ({app=~"api|web"} |= "keep" [1m])"#,
        &[
            ("0", "api", "3"),
            ("0", "web", "2"),
            ("1", "api", "3"),
            ("1", "web", "2"),
        ],
    ),
    (
        "approx/instant-metric",
        r#"approx_topk(1,sum by(app)(count_over_time({app=~"api|web"} |= "keep" [1m])))"#,
        &[("", "api", "3")],
    ),
    (
        "approx/instant-unlabelled-sum",
        r#"approx_topk(1,sum(count_over_time({app=~"api|web"} |= "keep" [1m])))"#,
        &[("", "", "5")],
    ),
    (
        "approx/missing-selector",
        r#"approx_topk(1,count_over_time({app="missing"}[1m]))"#,
        &[],
    ),
];

fn expected(timestamp: f64, items: &[(&str, &str, &str)]) -> Value {
    let mut rows = items
        .iter()
        .map(|(variant, app, value)| {
            let mut metric = serde_json::Map::new();
            if !variant.is_empty() {
                metric.insert("__variant__".into(), json!(variant));
            }
            if !app.is_empty() {
                metric.insert("app".into(), json!(app));
            }
            json!({"metric":metric,"points":[[timestamp,value]]})
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(ToString::to_string);
    json!({"samples":rows,"warnings":[]})
}

pub fn cases(base: i64) -> Vec<Case> {
    let timestamp = (base + 40_000_000_000)
        .to_f64()
        .expect("fixture timestamp converts to seconds")
        / 1e9;
    let mut result = INSTANT_CASES
        .iter()
        .map(|(id, query, rows)| Case {
            id,
            query,
            tenant: "experimental",
            range: false,
            expected_status: 200,
            expected: expected(timestamp, rows),
        })
        .collect::<Vec<_>>();
    result.push(Case {
        id:"variants/without-range-drops-unlabelled-series",
        query:r#"variants(sum without(app,service_name,detected_level,detected_level_extracted)(count_over_time({app="ignored"}[1m]))) of ({app=~"api|web"} |= "keep" [1m])"#,
        tenant:"experimental",range:true,expected_status:200,expected:expected(timestamp, &[]),
    });
    result.push(Case {
        id: "variants/absent-range-discards-unlabelled-synthetic-series",
        query: r#"variants(absent_over_time({app="original"}[1m])) of ({app="missing"}[1m])"#,
        tenant: "experimental",
        range: true,
        expected_status: 200,
        expected: expected(timestamp, &[]),
    });
    for (id, tenant, query, rows) in [
        (
            "discovery/disabled-does-not-synthesize-unknown",
            "experimental-discovery-disabled",
            r#"sum(count_over_time({app="discovery"} | detected_level="unknown" [1m]))"#,
            &[][..],
        ),
        (
            "discovery/custom-logfmt-field",
            "experimental-discovery-custom",
            r#"sum(count_over_time({app="discovery"} | detected_level="debug" [1m]))"#,
            &[("", "", "1")][..],
        ),
        (
            "discovery/bounded-json-depth",
            "experimental-discovery-custom",
            r#"sum(count_over_time({app="discovery"} | detected_level="info" [1m]))"#,
            &[][..],
        ),
        (
            "discovery/unlimited-json-depth",
            "experimental-discovery-deep",
            r#"sum(count_over_time({app="discovery"} | detected_level="info" [1m]))"#,
            &[("", "", "1")][..],
        ),
    ] {
        result.push(Case {
            id,
            query,
            tenant,
            range: false,
            expected_status: 200,
            expected: expected(timestamp, rows),
        });
    }
    let query = r#"variants(count_over_time({app="ignored"}[1m]),sum(count_over_time({app="ignored"}[1m]))) of ({app=~"api|web"} |= "keep" [1m])"#;
    let mut range_expected = expected(timestamp, &[("1", "", "5")]);
    range_expected["samples"][0]["points"]
        .as_array_mut()
        .unwrap()
        .push(json!([timestamp + 1.0, "5"]));
    range_expected["warnings"] = json!(["maximum of series (2) reached for variant (0)"]);
    result.push(Case {
        id: "variants/range-exact-cap-removes-earlier-points",
        query,
        tenant: "experimental",
        range: true,
        expected_status: 200,
        expected: range_expected,
    });
    let mut one = expected(timestamp, &[("1", "", "5")]);
    one["warnings"] = json!(["maximum of series (1) reached for variant (0)"]);
    result.push(Case {
        id: "variants/one-series-cap",
        query,
        tenant: "experimental-one",
        range: false,
        expected_status: 200,
        expected: one,
    });
    result.push(Case { id:"variants/zero-series-cap", query, tenant:"experimental-zero", range:false, expected_status:200, expected:json!({"samples":[],"warnings":["maximum of series (0) reached for variant (0)","maximum of series (0) reached for variant (1)"]}) });
    result.push(Case {
        id: "variants/disabled",
        query,
        tenant: "experimental-disabled",
        range: false,
        expected_status: 400,
        expected: json!("multi variant queries are disabled for this instance"),
    });
    result.push(Case {
        id: "approx/instant-vector",
        query: "approx_topk(1,vector(3))",
        tenant: "experimental",
        range: false,
        expected_status: 400,
        expected: json!(UNLABELLED_APPROX_ERROR),
    });
    result.push(Case {
        id: "approx/zero-k",
        query: r#"approx_topk(0,count_over_time({app=~"api|web"} |= "keep" [1m]))"#,
        tenant: "experimental",
        range: false,
        expected_status: 400,
        expected: json!("parse error : invalid parameter (must be greater than 0) approx_topk(0"),
    });
    result.push(Case {
        id: "approx/disabled",
        query: r#"approx_topk(1,count_over_time({app="api"}[1m]))"#,
        tenant: "experimental-disabled",
        range: false,
        expected_status: 500,
        expected: json!("approx_topk is not enabled. See -limits.shard_aggregations"),
    });
    result.push(Case {
        id: "approx/range-rejected",
        query: r#"approx_topk(1,count_over_time({app="api"}[1m]))"#,
        tenant: "experimental",
        range: true,
        expected_status: 500,
        expected: json!("count min sketches are only supported on instant queries"),
    });
    result.push(Case {
        id: "variants/tenant-isolation",
        query,
        tenant: "experimental-empty",
        range: false,
        expected_status: 200,
        expected: expected(timestamp, &[]),
    });
    result.extend(federation_cases(timestamp));
    result
}

// Exact cross-server comparison retains every output label. The independent
// fixture additionally checks app/variant identities, timestamps, values and
// warnings; unused discovery/structured-metadata labels are not its subject.
pub fn semantic_samples(value: &Value) -> Value {
    let mut rows = value["data"]["result"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            let mut metric = serde_json::Map::new();
            for name in ["__variant__", "app", "__tenant_id__"] {
                if let Some(label) = row["metric"].get(name) {
                    metric.insert(name.into(), label.clone());
                }
            }
            let points = row["values"]
                .as_array()
                .map_or_else(|| vec![row["value"].clone()], Clone::clone)
                .into_iter()
                .map(|point| json!([point[0].as_f64().unwrap(), point[1]]))
                .collect::<Vec<_>>();
            json!({"metric":metric,"points":points})
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(ToString::to_string);
    json!({"samples":rows,"warnings":value.get("warnings").cloned().unwrap_or_else(||json!([]))})
}

fn federation_cases(timestamp: f64) -> Vec<Case> {
    let by_app = r#"variants(sum by(app)(count_over_time({app="ignored"}[1m]))) of ({app=~"api|web"} |= "keep" [1m])"#;
    let global_budget = r#"variants(count_over_time({app="ignored"}[1m]),sum(count_over_time({app="ignored"}[1m]))) of ({app=~"api|web"} |= "keep" [1m])"#;
    let mut capped = expected(timestamp, &[("1", "", "10")]);
    capped["warnings"] = json!(["maximum of series (1) reached for variant (0)"]);
    let mut labels = expected(timestamp, &[("0", "", "5"), ("0", "", "5")]);
    labels["samples"][0]["metric"]["__tenant_id__"] = json!("experimental");
    labels["samples"][1]["metric"]["__tenant_id__"] = json!("experimental-disabled");
    labels["samples"]
        .as_array_mut()
        .unwrap()
        .sort_by_key(ToString::to_string);
    vec![
        Case {
            id: "variants/federated-any-enabled-aggregates-input",
            query: by_app,
            tenant: "experimental|experimental-disabled",
            range: false,
            expected_status: 200,
            expected: expected(timestamp, &[("0", "api", "6"), ("0", "web", "4")]),
        },
        Case {
            id: "variants/federated-all-disabled",
            query: by_app,
            tenant: "experimental-disabled|experimental-disabled2",
            range: false,
            expected_status: 400,
            expected: json!("multi variant queries are disabled for this instance"),
        },
        Case {
            id: "variants/federated-minimum-global-budget",
            query: global_budget,
            tenant: "experimental|experimental-one",
            range: false,
            expected_status: 200,
            expected: capped,
        },
        Case {
            id: "variants/federated-tenant-selector",
            query: r#"variants(sum by(app)(count_over_time({app="ignored"}[1m]))) of ({app=~"api|web",__tenant_id__="experimental-disabled"} |= "keep" [1m])"#,
            tenant: "experimental|experimental-disabled",
            range: false,
            expected_status: 200,
            expected: expected(timestamp, &[]),
        },
        Case {
            id: "variants/federated-tenant-label-grouping",
            query: r#"variants(sum by(__tenant_id__)(count_over_time({app="ignored"}[1m]))) of ({app=~"api|web"} |= "keep" [1m])"#,
            tenant: "experimental|experimental-disabled",
            range: false,
            expected_status: 200,
            expected: labels,
        },
        Case {
            id: "variants/federated-unmatched-tenant-selector",
            query: r#"variants(count_over_time({app="ignored"}[1m])) of ({app="api",__tenant_id__="missing"}[1m])"#,
            tenant: "experimental|experimental-disabled",
            range: false,
            expected_status: 200,
            expected: expected(timestamp, &[]),
        },
        Case {
            id: "approx/federated-merges-before-topk",
            query: r#"approx_topk(1,sum by(app)(count_over_time({app=~"api|web"} |= "keep" [1m])))"#,
            tenant: "experimental|experimental-one",
            range: false,
            expected_status: 200,
            expected: expected(timestamp, &[("", "api", "6")]),
        },
        Case {
            id: "approx/federated-intersection-gate",
            query: r#"approx_topk(1,count_over_time({app="api"}[1m]))"#,
            tenant: "experimental|experimental-disabled",
            range: false,
            expected_status: 500,
            expected: json!("approx_topk is not enabled. See -limits.shard_aggregations"),
        },
    ]
}
