use super::*;

#[derive(bon::Builder)]
struct HistogramBucket {
    upper_bound: &'static str,
    cumulative_count: f64,
}

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

    let direct_response = annotated_query_body(direct, &uri).await;
    let frontend_response = annotated_query_body(frontend, &uri).await;
    let direct_status = direct_response.status;
    let direct_body = direct_response.body;
    let frontend_status = frontend_response.status;
    let frontend_body = frontend_response.body;

    let want = serde_json::json!({
        "status": "success",
        "data": {"resultType": "matrix", "result": []},
        "warnings": [
            "PromQL warning: bucket label \"le\" is missing or has a malformed value of \"\" for metric name \"up\" (1:25)"
        ],
    });
    check!(direct_status == StatusCode::OK);
    check!(frontend_status == StatusCode::OK);
    check!(direct_body == want);
    check!(frontend_body == want);
}

#[tokio::test]
pub(crate) async fn monotonicity_details_survive_frontend_splits_and_cached_responses() {
    let mut store = InMemoryMetricStore::new();
    for bucket in [
        HistogramBucket::builder()
            .upper_bound("0.5")
            .cumulative_count(40.0)
            .build(),
        HistogramBucket::builder()
            .upper_bound("1")
            .cumulative_count(30.0)
            .build(),
        HistogramBucket::builder()
            .upper_bound("+Inf")
            .cumulative_count(35.0)
            .build(),
    ] {
        let mut labels = Labels::new();
        labels.insert("__name__", "h_bucket");
        labels.insert("le", bucket.upper_bound);
        store.push_float("tenant-a", labels, 0, bucket.cumulative_count);
    }
    let engine = PromqlEngine::new(Arc::new(store.clone()), EngineOpts::default());
    let query = "histogram_quantile(0.5, h_bucket)";
    let base = "PromQL info: input to histogram_quantile needed to be fixed for monotonicity (see https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile) for metric name \"h_bucket\"";
    let annotated = engine
        .query_instant_with_annotations(
            &krabka_blockstore::TenantId::new("tenant-a").unwrap(),
            query,
            0,
        )
        .await
        .unwrap();
    check!(annotated.annotations.infos == vec![format!("{base} (1:25)")]);
    let direct = Arc::new(PrometheusApiState::new(
        Arc::new(store.clone()),
        EngineOpts::default(),
    ));
    let frontend = Arc::new(
        PrometheusApiState::new(Arc::new(store), EngineOpts::default()).with_query_frontend(
            QueryFrontendOptions {
                split_interval: secs(30),
                shard_count: 1,
            },
        ),
    );
    let form = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("query", query)
        .append_pair("start", "0")
        .append_pair("end", "60")
        .append_pair("step", "30")
        .finish();
    let uri = format!("/api/v1/query_range?{form}");
    let direct_response = annotated_query_body(direct, &uri).await;
    let status = direct_response.status;
    let direct_body = direct_response.body;
    check!(status == StatusCode::OK);
    check!(
        direct_body["infos"]
            == serde_json::json!([format!(
                "{base}, from buckets 1 to +Inf, with a max diff of 10, over 3 samples from 1970-01-01T00:00:00Z to 1970-01-01T00:01:00Z (1:25)"
            )])
    );
    for _ in 0..2 {
        let response = annotated_query_body(frontend.clone(), &uri).await;
        let status = response.status;
        let body = response.body;
        check!(status == StatusCode::OK);
        check!(body == direct_body);
    }
}
