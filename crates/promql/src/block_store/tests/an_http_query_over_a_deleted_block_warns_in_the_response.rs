use super::*;

#[tokio::test]
pub(crate) async fn an_http_query_over_a_deleted_block_warns_in_the_response() {
    // M6 retention and compaction-input deletion remove block objects that an
    // index entry still names. The query answers with the blocks it can read and
    // reports the missing one as a `PromQL` warning. That warning has to reach
    // Grafana, so the HTTP envelope carries it.
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let mut block_store = BlockStore::new(object_store.clone(), base);

    let deleted_series = labels(&[("__name__", "up"), ("job", "api")]);
    let kept_series = labels(&[("__name__", "up"), ("job", "db")]);
    write_float_block(
        &mut block_store,
        "metrics/float/0001.parquet",
        &deleted_series,
        1_000,
        1.0,
    )
    .await;
    write_float_block(
        &mut block_store,
        "metrics/float/0002.parquet",
        &kept_series,
        1_000,
        2.0,
    )
    .await;
    object_store
        .delete(&ObjectPath::from("metrics/float/0001.parquet"))
        .await
        .unwrap();

    let state = Arc::new(PrometheusApiState::new(
        Arc::new(MetricBlockStore::new(block_store)),
        EngineOpts::default(),
    ));
    let router = authenticate_requests(prometheus_router(state), &ServerSecurity::default());
    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v1/query?time=1&query=up")
                .header("x-scope-orgid", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    check!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let mut body: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // The warning text embeds the object-store error for the deleted object, and
    // that message is not stable text. Take the array out and compare the rest of
    // the envelope as one value.
    let warnings = body
        .as_object_mut()
        .expect("response envelope")
        .remove("warnings")
        .expect("warnings key on a query that skipped a block");
    check!(
        body == serde_json::json!({
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [{
                    "metric": {"__name__": "up", "job": "db"},
                    "value": [1, "2"],
                }],
            },
        })
    );
    let warnings = warnings.as_array().expect("warnings array");
    check!(warnings.len() == 1);
    let warning = warnings[0].as_str().expect("warning string");
    check!(warning.contains("metrics/float/0001.parquet"));
    check!(warning.contains("missing"));
}
