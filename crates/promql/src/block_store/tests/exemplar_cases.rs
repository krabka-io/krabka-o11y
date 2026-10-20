use super::*;

mod exemplars_answer_around_a_deleted_block_and_warn {
    use super::*;

    #[tokio::test]
    pub(crate) async fn exemplars_answer_around_a_deleted_block_and_warn() {
        let fixture = MemoryBlockStoreFixture::build();
        let object_store = &fixture.object_store;
        let writer_store = fixture.writer();

        let deleted_series = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "http_requests_total"),
            crate::test_support::TestLabelPair::new("job", "api"),
            crate::test_support::TestLabelPair::new("pod", "one"),
        ]);
        let kept_series = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "http_requests_total"),
            crate::test_support::TestLabelPair::new("job", "api"),
            crate::test_support::TestLabelPair::new("pod", "two"),
        ]);
        let deleted = sidecar_manifest(
            &writer_store,
            MetricBlockKind::Exemplars,
            "metrics/exemplars/0001.parquet",
            exemplar_schema(),
            exemplar_batch(ExemplarRow {
                fingerprint: deleted_series.fingerprint(),
                timestamp_ms: 10_100,
                value: 1.0,
                trace_id: Some("aaa".to_string()),
                span_id: Some("bbb".to_string()),
                labels: vec![krabka_blockstore::LabelPair::new(
                    "kind",
                    "slow".to_string(),
                )],
            }),
            &deleted_series,
        )
        .await;
        let kept = sidecar_manifest(
            &writer_store,
            MetricBlockKind::Exemplars,
            "metrics/exemplars/0002.parquet",
            exemplar_schema(),
            exemplar_batch(ExemplarRow {
                fingerprint: kept_series.fingerprint(),
                timestamp_ms: 10_500,
                value: 7.0,
                trace_id: Some("ccc".to_string()),
                span_id: Some("ddd".to_string()),
                labels: vec![krabka_blockstore::LabelPair::new(
                    "kind",
                    "slow".to_string(),
                )],
            }),
            &kept_series,
        )
        .await;
        object_store
            .delete(&ObjectPath::from("metrics/exemplars/0001.parquet"))
            .await
            .unwrap();

        let store =
            MetricBlockStore::from_compaction_manifests(fixture.reader(), None, &[deleted, kept]);
        let scan = api_exemplar_scan(&store).await;

        check!(scan.exemplars.len() == 1);
        check!(scan.exemplars[0].series_labels == kept_series);
        check!(scan.exemplars[0].ts_ms == 10_500);
        check!(scan.warnings.len() == 1);
        check!(scan.warnings[0].contains("metrics/exemplars/0001.parquet"));
        check!(scan.warnings[0].contains("missing"));
    }
}

mod exemplars_include_closed_range_boundaries_and_filter_outside_rows {
    use super::*;

    #[derive(bon::Builder)]
    struct ExemplarFixture {
        timestamp_ms: i64,
        value: f64,
        trace_id: &'static str,
        span_id: &'static str,
        kind: &'static str,
    }

    struct ExpectedExemplar {
        row: usize,
        trace_id: &'static str,
        span_id: &'static str,
        timestamp_ms: i64,
        value: f64,
    }

    #[tokio::test]
    pub(crate) async fn exemplars_include_closed_range_boundaries_and_filter_outside_rows() {
        let series_labels = crate::test_support::labels::http_requests_total_api_labels();
        let fp = series_labels.fingerprint();
        let fixtures = [
            ExemplarFixture::builder()
                .timestamp_ms(9_999)
                .value(1.0)
                .trace_id("too-low")
                .span_id("s1")
                .kind("outside")
                .build(),
            ExemplarFixture::builder()
                .timestamp_ms(10_000)
                .value(2.0)
                .trace_id("start")
                .span_id("s2")
                .kind("inside")
                .build(),
            ExemplarFixture::builder()
                .timestamp_ms(11_000)
                .value(3.0)
                .trace_id("end")
                .span_id("s3")
                .kind("inside")
                .build(),
            ExemplarFixture::builder()
                .timestamp_ms(11_001)
                .value(4.0)
                .trace_id("too-high")
                .span_id("s4")
                .kind("outside")
                .build(),
        ];
        let rows = fixtures.map(|fixture| ExemplarRow {
            fingerprint: fp,
            timestamp_ms: fixture.timestamp_ms,
            value: fixture.value,
            trace_id: Some(fixture.trace_id.to_string()),
            span_id: Some(fixture.span_id.to_string()),
            labels: vec![krabka_blockstore::LabelPair::new(
                "kind",
                fixture.kind.to_string(),
            )],
        });
        let batch = encode_exemplar_rows(&rows).unwrap();
        let store = manifest_store(ManifestBlock {
            kind: MetricBlockKind::Exemplars,
            block_key: "metrics/exemplars/0005.parquet",
            schema: exemplar_schema(),
            batch,
            last_offset: 3,
            series_labels: series_labels.to_storage_labels().unwrap(),
        })
        .await;
        let exemplars = api_exemplars(&store).await;

        check!(exemplars.len() == 2);
        let expected = [
            ExpectedExemplar {
                row: 0,
                trace_id: "start",
                span_id: "s2",
                timestamp_ms: 10_000,
                value: 2.0,
            },
            ExpectedExemplar {
                row: 1,
                trace_id: "end",
                span_id: "s3",
                timestamp_ms: 11_000,
                value: 3.0,
            },
        ];
        for expected_index in 0..expected.len() {
            let case = &expected[expected_index];
            let row = case.row;
            check!(exemplars[row].series_labels == series_labels, "row {row}");
            check!(
                exemplars[row].labels.get("trace_id") == Some(case.trace_id),
                "row {row}"
            );
            check!(
                exemplars[row].labels.get("span_id") == Some(case.span_id),
                "row {row}"
            );
            check!(
                exemplars[row].labels.get("kind") == Some("inside"),
                "row {row}"
            );
            check!(exemplars[row].ts_ms == case.timestamp_ms, "row {row}");
            check!(
                exemplars[row].value.to_bits() == case.value.to_bits(),
                "row {row}"
            );
        }
    }
}

mod exemplars_reads_compacted_exemplar_sidecar_blocks {
    use super::*;

    #[tokio::test]
    pub(crate) async fn exemplars_reads_compacted_exemplar_sidecar_blocks() {
        let series_labels = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "http_requests_total"),
            crate::test_support::TestLabelPair::new("job", "api"),
        ]);
        let batch = exemplar_batch(ExemplarRow {
            fingerprint: series_labels.fingerprint(),
            timestamp_ms: 10_500,
            value: 7.0,
            trace_id: Some("abc".to_string()),
            span_id: Some("def".to_string()),
            labels: vec![krabka_blockstore::LabelPair::new(
                "kind",
                "slow".to_string(),
            )],
        });
        let store = manifest_store(ManifestBlock {
            kind: MetricBlockKind::Exemplars,
            block_key: "metrics/exemplars/0003.parquet",
            schema: exemplar_schema(),
            batch,
            last_offset: 0,
            series_labels: series_labels.clone(),
        })
        .await;
        let exemplars = api_exemplars(&store).await;

        check!(exemplars.len() == 1);
        check!(exemplars[0].series_labels == series_labels);
        check!(exemplars[0].labels.get("trace_id") == Some("abc"));
        check!(exemplars[0].labels.get("span_id") == Some("def"));
        check!(exemplars[0].labels.get("kind") == Some("slow"));
        check!(exemplars[0].ts_ms == 10_500);
        check!((exemplars[0].value - 7.0).abs() < f64::EPSILON);
    }
}
