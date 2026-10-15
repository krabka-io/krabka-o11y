use super::*;

mod info_function_adds_target_info_data_labels_by_job_and_instance {
    use super::*;

    #[tokio::test]
    pub(crate) async fn info_function_adds_target_info_data_labels_by_job_and_instance() {
        let engine = target_info_engine();
        let samples = instant_vector(&engine, "info(http_requests_total)", 10_000).await;
        check!(samples.len() == 1);
        check!(samples[0].labels.get("__name__") == Some("http_requests_total"));
        check!(samples[0].labels.get("job") == Some("api"));
        check!(samples[0].labels.get("instance") == Some("a"));
        check!(samples[0].labels.get("region") == Some("east"));
        check!(samples[0].labels.get("cluster") == Some("prod"));
        check!(approx_eq(float_value(&samples[0].value), 7.0));
    }
}

mod info_function_drops_series_when_required_data_label_selector_does_not_match {
    use super::*;

    #[tokio::test]
    pub(crate) async fn info_function_drops_series_when_required_data_label_selector_does_not_match()
     {
        let engine = info_metrics_engine(vec![super::target_info_engine::info_series_labels(
            "target_info",
            &[("region", "east")],
        )]);
        let samples = instant_vector(
            &engine,
            r#"info(http_requests_total, {region="west"})"#,
            10_000,
        )
        .await;
        assert2::assert!(samples.is_empty());
    }
}

mod info_function_keeps_base_label_when_info_label_overlaps {
    use super::*;

    #[tokio::test]
    pub(crate) async fn info_function_keeps_base_label_when_info_label_overlaps() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            super::target_info_engine::request_metric_labels(&[("region", "base")]),
            10_000,
            7.0,
        );
        store.push_float(
            "tenant-a",
            super::target_info_engine::info_series_labels(
                "target_info",
                &[("region", "info"), ("cluster", "prod")],
            ),
            10_000,
            1.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let samples = instant_vector(&engine, "info(http_requests_total)", 10_000).await;
        assert2::assert!(samples.len() == 1);
        assert2::assert!(samples[0].labels.get("region") == Some("base"));
        assert2::assert!(samples[0].labels.get("cluster") == Some("prod"));
    }
}

mod info_function_merges_data_labels_from_multiple_info_metrics {
    use super::*;

    #[tokio::test]
    pub(crate) async fn info_function_merges_data_labels_from_multiple_info_metrics() {
        let engine = info_metrics_engine(vec![
            super::target_info_engine::info_series_labels("target_info", &[("cluster", "prod")]),
            super::target_info_engine::info_series_labels("build_info", &[("version", "1.2.3")]),
        ]);
        let samples = instant_vector(
            &engine,
            r#"info(http_requests_total, {__name__=~".+_info"})"#,
            10_000,
        )
        .await;
        assert2::assert!(samples.len() == 1);
        assert2::assert!(samples[0].labels.get("cluster") == Some("prod"));
        assert2::assert!(samples[0].labels.get("version") == Some("1.2.3"));
    }
}

mod info_function_uses_data_label_selector_to_filter_and_copy_labels {
    use super::*;

    #[tokio::test]
    pub(crate) async fn info_function_uses_data_label_selector_to_filter_and_copy_labels() {
        let engine = target_info_engine();
        let samples = query_instant_vector(
            &engine,
            "tenant-a",
            r#"info(http_requests_total, {region="east"})"#,
            10_000,
        )
        .await;
        assert2::assert!(samples.len() == 1);
        assert2::assert!(
            samples[0].labels.clone()
                == labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "http_requests_total"),
                    crate::test_support::TestLabelPair::new("job", "api"),
                    crate::test_support::TestLabelPair::new("instance", "a"),
                    crate::test_support::TestLabelPair::new("region", "east")
                ])
        );
        assert2::assert!(approx_eq(float_value(&samples[0].value), 7.0));
    }
}

mod info_function_uses_named_info_metric_selector {
    use super::*;

    #[tokio::test]
    pub(crate) async fn info_function_uses_named_info_metric_selector() {
        let engine = info_metrics_engine(vec![super::target_info_engine::info_series_labels(
            "build_info",
            &[("version", "1.2.3")],
        )]);
        let samples = instant_vector(
            &engine,
            r#"info(http_requests_total, {__name__="build_info"})"#,
            10_000,
        )
        .await;
        assert2::assert!(samples.len() == 1);
        assert2::assert!(samples[0].labels.get("version") == Some("1.2.3"));
    }
}
