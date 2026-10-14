use super::*;

struct QueryAnnotationCase<'a> {
    query: &'static str,
    warnings: &'a [&'a str],
    infos: &'a [&'a str],
}

struct QueryRouteCase {
    route: &'static str,
    result_type: &'static str,
}

#[tokio::test]
pub(crate) async fn query_annotations_reach_the_response_envelope() {
    // Prometheus 3.8.0 answers 200 and adds a top-level `warnings` array for its
    // `PromQLWarning`-class annotations and a top-level `infos` array for its
    // `PromQLInfo`-class ones. It omits each key when that class raised nothing.
    // Every query here returns an empty result, so the whole envelope is
    // predictable and the test compares it as one value.
    let no_annotations: Vec<&str> = Vec::new();
    let bad_bucket_label = vec![
        "PromQL warning: bucket label \"le\" is missing or has a malformed value of \"\" for metric name \"up\" (1:25)",
    ];
    let incompatible_types = vec![
        "PromQL info: incompatible sample types encountered for binary operator \">\": histogram > float (1:1)",
    ];
    let cases = [
        QueryAnnotationCase {
            query: "up{job=\"none\"}",
            warnings: &no_annotations,
            infos: &no_annotations,
        },
        QueryAnnotationCase {
            query: "histogram_quantile(0.5, up)",
            warnings: &bad_bucket_label,
            infos: &no_annotations,
        },
        QueryAnnotationCase {
            query: "h > 80",
            warnings: &no_annotations,
            infos: &incompatible_types,
        },
    ];

    for case in cases {
        for route_case in [
            QueryRouteCase {
                route: "/api/v1/query",
                result_type: "vector",
            },
            QueryRouteCase {
                route: "/api/v1/query_range",
                result_type: "matrix",
            },
        ] {
            let state = Arc::new(PrometheusApiState::new(
                Arc::new(annotation_store()),
                EngineOpts::default(),
            ));
            let response =
                annotated_query_body(state, &annotation_query_uri(route_case.route, case.query))
                    .await;
            let status = response.status;
            let body = response.body;

            let mut want = serde_json::Map::new();
            want.insert("status".to_string(), serde_json::json!("success"));
            want.insert(
                "data".to_string(),
                serde_json::json!({"resultType": route_case.result_type, "result": []}),
            );
            if !case.warnings.is_empty() {
                want.insert("warnings".to_string(), serde_json::json!(case.warnings));
            }
            if !case.infos.is_empty() {
                want.insert("infos".to_string(), serde_json::json!(case.infos));
            }

            check!(
                status == StatusCode::OK,
                "{} on {}",
                case.query,
                route_case.route
            );
            check!(
                body == serde_json::Value::Object(want),
                "{} on {}",
                case.query,
                route_case.route
            );
        }
    }
}
