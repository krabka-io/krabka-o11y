use super::*;

#[tokio::test]
pub(crate) async fn query_annotations_reach_the_response_envelope() {
    // Prometheus 3.8.0 answers 200 and adds a top-level `warnings` array for its
    // `PromQLWarning`-class annotations and a top-level `infos` array for its
    // `PromQLInfo`-class ones. It omits each key when that class raised nothing.
    // Every query here returns an empty result, so the whole envelope is
    // predictable and the test compares it as one value.
    let no_annotations: Vec<&str> = Vec::new();
    let bad_bucket_label = vec![
        "PromQL warning: bucket label \"le\" is missing or has a malformed value of \"\" for metric name \"up\"",
    ];
    let incompatible_types = vec![
        "PromQL info: incompatible sample types encountered for binary operator \">\": histogram > float",
    ];
    let cases = [
        ("up{job=\"none\"}", &no_annotations, &no_annotations),
        (
            "histogram_quantile(0.5, up)",
            &bad_bucket_label,
            &no_annotations,
        ),
        ("h > 80", &no_annotations, &incompatible_types),
    ];

    for (query, warnings, infos) in cases {
        for (route, result_type) in [
            ("/api/v1/query", "vector"),
            ("/api/v1/query_range", "matrix"),
        ] {
            let state = Arc::new(PrometheusApiState::new(
                Arc::new(annotation_store()),
                EngineOpts::default(),
            ));
            let (status, body) =
                annotated_query_body(state, &annotation_query_uri(route, query)).await;

            let mut want = serde_json::Map::new();
            want.insert("status".to_string(), serde_json::json!("success"));
            want.insert(
                "data".to_string(),
                serde_json::json!({"resultType": result_type, "result": []}),
            );
            if !warnings.is_empty() {
                want.insert("warnings".to_string(), serde_json::json!(warnings));
            }
            if !infos.is_empty() {
                want.insert("infos".to_string(), serde_json::json!(infos));
            }

            check!(status == StatusCode::OK, "{query} on {route}");
            check!(
                body == serde_json::Value::Object(want),
                "{query} on {route}"
            );
        }
    }
}
