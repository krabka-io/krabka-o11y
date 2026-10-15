use super::*;

mod a_block_deleted_after_the_manifest_listing_is_skipped_and_warned {
use krabka_metrics::{
    FloatRow, ObjectStoreCompactionIndexSink, list_compaction_manifests,
    write_compacted_tenant_blocks,
};

use super::*;
use crate::AnnotatedQueryResult;

struct DeletedBlockQueryCase {
    query: &'static str,
    warm: bool,
}

/// Writes one float block per offset in `offsets`, each with one sample of
/// `soak_metric{block="<offset>"}`.
async fn write_blocks(object_store: &Arc<dyn ObjectStore>, offsets: &[i64]) {
    let writer = krabka_blockstore::BlockWriter::new(Arc::clone(object_store));
    let sink = ObjectStoreCompactionIndexSink::new(Arc::clone(object_store));
    for &offset in offsets {
        let series = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "soak_metric"),
            crate::test_support::TestLabelPair::new("block", &offset.to_string()),
        ]);
        let rows = compaction_rows_for_series(
            &series,
            vec![FloatRow {
                fingerprint: series.fingerprint(),
                timestamp_ms: 90_000,
                value: 1.0,
                start_timestamp_ms: None,
            }],
            Vec::new(),
        );
        write_compacted_tenant_blocks(&writer, &sink, &rows, offset, offset)
            .await
            .unwrap();
    }
}

#[tokio::test]
pub(crate) async fn a_block_deleted_after_the_manifest_listing_is_skipped_and_warned() {
    // `warm` runs one query before the delete, so the deleted block's footer
    // is in the footer cache when the second query reads it.
    for case in [
        DeletedBlockQueryCase {
            query: "count(count_over_time(soak_metric[60s]))",
            warm: false,
        },
        DeletedBlockQueryCase {
            query: "count(count_over_time(soak_metric[60s]))",
            warm: true,
        },
        DeletedBlockQueryCase {
            query: "sum(last_over_time(soak_metric[60s]))",
            warm: false,
        },
        DeletedBlockQueryCase {
            query: "sum(last_over_time(soak_metric[60s]))",
            warm: true,
        },
        DeletedBlockQueryCase {
            query: "avg(last_over_time(soak_metric[60s]))",
            warm: false,
        },
        DeletedBlockQueryCase {
            query: "avg(last_over_time(soak_metric[60s]))",
            warm: true,
        },
    ] {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        write_blocks(&object_store, &[1, 2]).await;
        let manifests = list_compaction_manifests(&object_store).await.unwrap();
        let deleted = manifests
            .iter()
            .find(|manifest| manifest.first_offset == 1)
            .unwrap()
            .block_key
            .clone();
        let base = url::Url::parse("memory:///").unwrap();
        let store = MetricBlockStore::from_compaction_manifests(
            BlockStore::new(Arc::clone(&object_store), base.clone()),
            Some(BlockStore::new(Arc::clone(&object_store), base)),
            &manifests,
        );
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        if case.warm {
            engine
                .query_instant(&tenant_id("tenant-a"), case.query, 100_000)
                .await
                .unwrap();
        }

        object_store
            .delete(&ObjectPath::from(deleted.as_str()))
            .await
            .unwrap();
        let annotated = engine
            .query_instant_with_annotations(&tenant_id("tenant-a"), case.query, 100_000)
            .await
            .unwrap();

        check!(
            annotated
                == AnnotatedQueryResult {
                    result: QueryResult::InstantVector(vec![InstantSample {
                        labels: Labels::new().into(),
                        ts_ms: 100_000,
                        value: SampleValue::Float(1.0),
                        drop_name: false,
                    }]),
                    annotations: Annotations {
                        // The engine appends the source position of the
                        // selector that the warning came from.
                        warnings: vec![format!(
                            "block \"{deleted}\" is missing from object storage and is not in \
                             this result (1:1)"
                        )],
                        infos: Vec::new(),
                        ..crate::Annotations::default()
                    },
                },
            "query={}, warm={}",
            case.query,
            case.warm
        );
    }
}

}

mod a_query_answers_around_a_deleted_block_and_warns {
use super::*;

struct ByteSeriesCase {
    offset: i64,
    raw: u8,
}

#[tokio::test]
pub(crate) async fn a_query_answers_around_a_deleted_block_and_warns() {
    let fixture = deleted_api_block_store().await;

    let InstantUpQuery {
        samples,
        annotations,
    } = instant_up_query(fixture.block_store).await;
    assert2::assert!(
        samples
            == vec![InstantSample {
                labels: fixture.kept_series.into(),
                ts_ms: 1_000,
                value: SampleValue::Float(2.0),
                drop_name: false,
            }]
    );
    check!(annotations.warnings.len() == 1);
    check!(annotations.warnings[0].contains("metrics/float/0001.parquet"));
    check!(annotations.warnings[0].contains("missing"));
    check!(annotations.infos.is_empty());
}

#[tokio::test]
async fn byte_series_selection_precedes_missing_block_warnings_for_every_kind() {
    use krabka_metrics::{
        ExemplarRow, FloatRow, MetadataRow, NativeHistogramRow, ObjectStoreCompactionIndexSink,
        TenantCompactionRows, write_compacted_tenant_blocks,
    };
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let writer_store = BlockStore::new(object_store.clone(), base.clone());
    let sink = ObjectStoreCompactionIndexSink::new(object_store.clone());
    let mut manifests = Vec::new();
    for case in [
        ByteSeriesCase {
            offset: 0,
            raw: 0xff,
        },
        ByteSeriesCase {
            offset: 1,
            raw: 0xfe,
        },
    ] {
        let mut series = crate::PromqlLabels::from_pairs([krabka_metrics::MetricLabelPair::new(
            "__name__",
            "byte_input",
        )]);
        series.insert("raw", crate::PromqlString::from(vec![case.raw]));
        let fingerprint = series.fingerprint();
        let histogram = krabka_metrics::NativeHistogram {
            schema: 0,
            is_float: false,
            reset_hint: krabka_metrics::ResetHint::No,
            zero_threshold: 0.0,
            zero_count: 1.0,
            count: 1.0,
            sum: 0.0,
            positive_spans: Vec::new(),
            positive_counts: Vec::new(),
            negative_spans: Vec::new(),
            negative_counts: Vec::new(),
            custom_values: None,
            start_timestamp_ms: None,
        };
        let rows = TenantCompactionRows {
            tenant: "tenant-a".into(),
            series_labels: {
                let mut labels_by_fingerprint = std::collections::BTreeMap::new();
                labels_by_fingerprint.insert(fingerprint, series);
                labels_by_fingerprint
            },
            float_rows: vec![FloatRow {
                fingerprint,
                timestamp_ms: 1_000,
                value: 2.0,
                start_timestamp_ms: None,
            }],
            histogram_rows: vec![NativeHistogramRow {
                fingerprint,
                timestamp_ms: 1_000,
                hist: histogram,
            }],
            exemplar_rows: vec![ExemplarRow {
                fingerprint,
                timestamp_ms: 1_000,
                value: 2.0,
                trace_id: Some("abc".into()),
                span_id: None,
                labels: Vec::new(),
            }],
            metadata_rows: vec![MetadataRow {
                fingerprint,
                metric_family_name: "byte_input".into(),
                metric_type: "gauge".into(),
                help: "fixture".into(),
                unit: String::new(),
            }],
            clock_rows: Vec::new(),
        };
        manifests.extend(
            write_compacted_tenant_blocks(
                &writer_store.writer(),
                &sink,
                &rows,
                case.offset,
                case.offset,
            )
            .await
            .unwrap()
            .into_iter()
            .map(|written| written.manifest),
        );
    }
    for manifest in &manifests {
        if manifest.first_offset == 1 {
            object_store
                .delete(&ObjectPath::from(manifest.block_key.as_str()))
                .await
                .unwrap();
        }
    }
    let store = MetricBlockStore::from_compaction_manifests(
        BlockStore::new(object_store.clone(), base.clone()),
        Some(BlockStore::new(object_store.clone(), base)),
        &manifests,
    );
    let selected = [crate::PromqlMatcher::new(
        "raw",
        krabka_blockstore::MatchOp::Eq,
        crate::PromqlString::from(vec![0xff]),
    )];
    let scan = store.scan("tenant-a", &selected, 0, 2_000).await.unwrap();
    assert2::assert!(
        scan.float_table.is_some() && scan.histogram_table.is_some() && scan.warnings.is_empty()
    );
    let all = store.scan("tenant-a", &[], 0, 2_000).await.unwrap();
    assert2::assert!(all.warnings.len() == 2);
    let exemplars = store
        .exemplars("tenant-a", &selected, 0, 2_000)
        .await
        .unwrap();
    assert2::assert!(
        exemplars.warnings.is_empty()
            && exemplars.exemplars.len() == 1
            && exemplars.exemplars[0].value.to_bits() == 2.0_f64.to_bits()
    );
    let all_exemplars = store.exemplars("tenant-a", &[], 0, 2_000).await.unwrap();
    assert2::assert!(all_exemplars.warnings.len() == 1 && all_exemplars.exemplars.len() == 1);
    let other = store
        .metadata("tenant-a", Some("absent_metric"))
        .await
        .unwrap();
    assert2::assert!(other.warnings.is_empty() && other.metadata.is_empty());
    let matching = store
        .metadata("tenant-a", Some("byte_input"))
        .await
        .unwrap();
    assert2::assert!(matching.warnings.len() == 1 && matching.metadata.len() == 1);
}

}

mod a_query_over_present_blocks_raises_no_warning {
use super::*;

#[tokio::test]
pub(crate) async fn a_query_over_present_blocks_raises_no_warning() {
    let mut block_store = MemoryBlockStoreFixture::build().writer();

    let first_series = labels(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]);
    let second_series = labels(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "db"),
    ]);
    write_float_block(
        &mut block_store,
        "metrics/float/0001.parquet",
        &first_series,
        1_000,
        1.0,
    )
    .await;
    write_float_block(
        &mut block_store,
        "metrics/float/0002.parquet",
        &second_series,
        1_000,
        2.0,
    )
    .await;

    let InstantUpQuery {
        samples,
        annotations,
    } = instant_up_query(block_store).await;
    check!(samples.len() == 2);
    check!(annotations == Annotations::new());
}

}

mod an_http_query_over_a_deleted_block_warns_in_the_response {
use super::*;

#[tokio::test]
pub(crate) async fn an_http_query_over_a_deleted_block_warns_in_the_response() {
    // M6 retention and compaction-input deletion remove block objects that an
    // index entry still names. The query answers with the blocks it can read and
    // reports the missing one as a `PromQL` warning. That warning has to reach
    // Grafana, so the HTTP envelope carries it.
    let fixture = deleted_api_block_store().await;

    let state = Arc::new(PrometheusApiState::new(
        Arc::new(MetricBlockStore::new(fixture.block_store)),
        EngineOpts::default(),
    ));
    let router = authenticate_requests(prometheus_router(state), &ServerSecurity::default());
    let response = router
        .oneshot(crate::test_support::http::empty_tenant_request(
            "/api/v1/query?time=1&query=up",
            "tenant-a",
        ))
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

}

mod metadata_answers_around_a_deleted_block_and_warns {
use super::*;

#[tokio::test]
pub(crate) async fn metadata_answers_around_a_deleted_block_and_warns() {
    let fixture = MemoryBlockStoreFixture::build();
    let object_store = &fixture.object_store;
    let writer_store = fixture.writer();

    // One metric family, described by two blocks. The index names both, and one
    // of the two objects is gone.
    let series = labels(&[crate::test_support::TestLabelPair::new(
        "__name__",
        "http_requests_total",
    )]);
    let deleted = sidecar_manifest(
        &writer_store,
        MetricBlockKind::Metadata,
        "metrics/metadata/0001.parquet",
        metadata_schema(),
        encode_metadata_rows(&[MetadataRow {
            fingerprint: series.fingerprint(),
            metric_family_name: "http_requests_total".to_string(),
            metric_type: "counter".to_string(),
            help: "Requests served, before the help text was corrected.".to_string(),
            unit: "requests".to_string(),
        }])
        .unwrap(),
        &series,
    )
    .await;
    let kept = sidecar_manifest(
        &writer_store,
        MetricBlockKind::Metadata,
        "metrics/metadata/0002.parquet",
        metadata_schema(),
        encode_metadata_rows(&[MetadataRow {
            fingerprint: series.fingerprint(),
            metric_family_name: "http_requests_total".to_string(),
            metric_type: "counter".to_string(),
            help: "Total HTTP requests.".to_string(),
            unit: "requests".to_string(),
        }])
        .unwrap(),
        &series,
    )
    .await;
    object_store
        .delete(&ObjectPath::from("metrics/metadata/0001.parquet"))
        .await
        .unwrap();

    let store =
        MetricBlockStore::from_compaction_manifests(fixture.reader(), None, &[deleted, kept]);
    let scan = store
        .metadata("tenant-a", Some("http_requests_total"))
        .await
        .unwrap();

    assert2::assert!(
        scan.metadata
            == vec![MetadataRecord {
                metric_family_name: "http_requests_total".to_string(),
                metric_type: "counter".to_string(),
                help: "Total HTTP requests.".to_string(),
                unit: "requests".to_string(),
            }]
    );
    check!(scan.warnings.len() == 1);
    check!(scan.warnings[0].contains("metrics/metadata/0001.parquet"));
    check!(scan.warnings[0].contains("missing"));
}

}

mod a_corrupt_block_still_fails_the_query {
use super::{deleted_api_block_store::api_db_block_store, *};

#[tokio::test]
pub(crate) async fn a_corrupt_block_still_fails_the_query() {
    let fixture = api_db_block_store().await;
    // The object is there and the bytes do not decode. That is damaged data,
    // not deleted data, so the query must fail rather than answer around it.
    fixture
        .object_store
        .put(
            &ObjectPath::from("metrics/float/0001.parquet"),
            PutPayload::from_static(b"this is not a parquet block"),
        )
        .await
        .unwrap();

    let store = MetricBlockStore::new(fixture.block_store);
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let error = engine
        .query_instant(&tenant_id("tenant-a"), "up", 1_000)
        .await
        .expect_err("a corrupt block fails the query");

    check!(matches!(error, PromqlError::Store(_)));
    check!(error.to_string().contains("metrics/float/0001.parquet"));
    check!(error.to_string().contains("corrupt"));
}

}
