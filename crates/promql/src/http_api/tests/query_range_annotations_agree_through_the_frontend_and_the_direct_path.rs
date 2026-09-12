use super::*;

#[tokio::test]
pub(crate) async fn query_range_annotations_agree_through_the_frontend_and_the_direct_path() {
    // The frontend splits this window into four sub-queries, and each sub-query
    // raises the same warning. The envelope must carry that warning once, and it
    // must match the envelope of the same query with no frontend configured.
    let form = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("query", "histogram_quantile(0.5, up)")
        .append_pair("start", "0")
        .append_pair("end", "360")
        .append_pair("step", "60")
        .finish();
    let uri = format!("/api/v1/query_range?{form}");

    let direct = Arc::new(PrometheusApiState::new(
        Arc::new(annotation_store()),
        EngineOpts::default(),
    ));
    let frontend = Arc::new(
        PrometheusApiState::new(Arc::new(annotation_store()), EngineOpts::default())
            .with_query_frontend(QueryFrontendOptions {
                split_interval: secs(120),
                shard_count: 1,
            }),
    );

    let (direct_status, direct_body) = annotated_query_body(direct, &uri).await;
    let (frontend_status, frontend_body) = annotated_query_body(frontend, &uri).await;

    let want = serde_json::json!({
        "status": "success",
        "data": {"resultType": "matrix", "result": []},
        "warnings": [
            "PromQL warning: bucket label \"le\" is missing or has a malformed value of \"\" for metric name \"up\""
        ],
    });
    check!(direct_status == StatusCode::OK);
    check!(frontend_status == StatusCode::OK);
    check!(direct_body == want);
    check!(frontend_body == want);
}
