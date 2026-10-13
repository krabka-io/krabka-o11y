//! Shared columnar block-store primitives for Krabka observability signals.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{self, Cursor},
    path::{Component, Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{
            Array, Int64Array, MapArray, MapBuilder, RecordBatch, StringArray, StringBuilder,
            UInt64Array,
        },
        datatypes::{DataType, Field, Fields, Schema},
        error::ArrowError,
    },
    catalog::Session,
    common::config::{ParquetOptions, TableParquetOptions},
    datasource::{
        TableProvider,
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        object_store::ObjectStoreUrl,
        provider::TableProviderFilterPushDown,
    },
    error::DataFusionError,
    logical_expr::{Expr, TableType},
    parquet::{
        arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
        errors::ParquetError,
    },
    physical_plan::ExecutionPlan,
    prelude::SessionContext,
};
use futures::StreamExt as _;
use krabka_units::prelude::*;
use object_store::{ObjectStore, ObjectStoreExt, path::Path as ObjectPath};
use regex::Regex;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::instrument;
use url::Url;
use xxhash_rust::xxh3::xxh3_64;

use crate::{path_escape::escape_object_path_segment, reader::DEFAULT_BLOCK_READ_MAX};

#[cfg(test)]
mod tests {

    /// The manifest path is where every reader looks for the log index. An
    /// empty `PathBuf` in its place points every caller at the store root.
    #[test]
    fn log_index_manifest_path_joins_the_relative_path() {
        let path = super::log_index_manifest_path("/var/lib/krabka");
        assert2::check!(path == std::path::Path::new("/var/lib/krabka/index/logs/manifest.json"));
    }
    use std::fmt::Write as _;

    use assert2::check;
    use datafusion::prelude::{col, lit};
    use object_store::{
        GetOptions, GetResult, local::LocalFileSystem, memory::InMemory, path::Path as ObjectPath,
    };

    use super::*;
    use crate::awkward_tenants::AWKWARD_TENANTS;

    struct LabelPredicateCase {
        name: &'static str,
        op: MatchOp,
        value: &'static str,
        expected: bool,
    }

    struct SeriesMatchCase<'a> {
        tenant: &'a str,
        predicates: &'a [LabelPredicate],
        expected: BTreeSet<SeriesFingerprint>,
    }

    struct TimeRangeOverlapCase {
        range: TimeRange,
        expected: bool,
    }

    struct BlockMatchCase<'a> {
        name: &'static str,
        tenant: &'static str,
        time_range: TimeRange,
        fingerprints: &'a [SeriesFingerprint],
        expected: Vec<BlockDescriptor>,
    }

    struct OutOfRangeTimestamp {
        description: &'static str,
        timestamp_ns: i64,
    }

    struct FilterPushdownCase<'a> {
        filter: &'a Expr,
        expected: TableProviderFilterPushDown,
    }

    struct PathExpectation {
        actual: ObjectPath,
        expected: &'static str,
    }

    struct ShardParseCase {
        name: &'static str,
        location: ObjectPath,
        expected: Option<TimeRange>,
    }

    #[test]
    fn labels_and_fingerprints_are_canonicalized_with_length_prefixes() {
        let label_set = labels([
            crate::labels::LabelPair::new("service", "api"),
            crate::labels::LabelPair::new("env", "prod"),
        ]);
        let expected = labels([
            crate::labels::LabelPair::new("env", "prod"),
            crate::labels::LabelPair::new("service", "api"),
        ]);

        assert2::assert!(label_set == expected);
        assert2::assert!(series_fingerprint(&expected) != 0);
        assert2::assert!(
            series_fingerprint(&labels([crate::labels::LabelPair::new("a", "bc")]))
                != series_fingerprint(&labels([crate::labels::LabelPair::new("ab", "c")]))
        );
    }

    #[test]
    fn label_predicates_match_exact_absent_and_anchored_regex_values() {
        let label_set = labels([
            crate::labels::LabelPair::new("service", "api"),
            crate::labels::LabelPair::new("env", "prod"),
        ]);
        let cases = [
            LabelPredicateCase {
                name: "service",
                op: MatchOp::Equal,
                value: "api",
                expected: true,
            },
            LabelPredicateCase {
                name: "service",
                op: MatchOp::Equal,
                value: "worker",
                expected: false,
            },
            LabelPredicateCase {
                name: "service",
                op: MatchOp::NotEqual,
                value: "worker",
                expected: true,
            },
            LabelPredicateCase {
                name: "cluster",
                op: MatchOp::NotEqual,
                value: "east",
                expected: true,
            },
            LabelPredicateCase {
                name: "service",
                op: MatchOp::RegexEqual,
                value: "api|worker",
                expected: true,
            },
            LabelPredicateCase {
                name: "service",
                op: MatchOp::RegexNotEqual,
                value: "api-.+",
                expected: true,
            },
            LabelPredicateCase {
                name: "cluster",
                op: MatchOp::RegexNotEqual,
                value: "east",
                expected: true,
            },
            LabelPredicateCase {
                name: "service",
                op: MatchOp::RegexEqual,
                value: "p",
                expected: false,
            },
        ];

        for case in cases {
            assert2::assert!(
                LabelPredicate::new(case.name, case.op, case.value)
                    .unwrap()
                    .matches(&label_set)
                    == case.expected
            );
        }
        check!(LabelPredicate::new("service", MatchOp::RegexEqual, "[").is_err());
    }

    #[test]
    fn label_index_filters_by_tenant_exact_postings_and_residual_predicates() {
        let mut index = LabelIndex::default();
        let api_prod_labels = labels([
            crate::labels::LabelPair::new("service", "api"),
            crate::labels::LabelPair::new("env", "prod"),
            crate::labels::LabelPair::new("region", "east"),
        ]);
        let api_stage_labels = labels([
            crate::labels::LabelPair::new("service", "api"),
            crate::labels::LabelPair::new("env", "stage"),
            crate::labels::LabelPair::new("region", "west"),
        ]);
        let worker_prod_labels = labels([
            crate::labels::LabelPair::new("service", "worker"),
            crate::labels::LabelPair::new("env", "prod"),
            crate::labels::LabelPair::new("region", "east"),
        ]);
        let other_tenant_labels = labels([
            crate::labels::LabelPair::new("service", "api"),
            crate::labels::LabelPair::new("env", "prod"),
            crate::labels::LabelPair::new("region", "north"),
        ]);
        let api_prod = index.insert_series("tenant-a", api_prod_labels.clone());
        let api_stage = index.insert_series("tenant-a", api_stage_labels.clone());
        let worker_prod = index.insert_series("tenant-a", worker_prod_labels.clone());
        let other_tenant = index.insert_series("tenant-b", other_tenant_labels.clone());
        let mut expected_tenant_a_series = vec![
            TenantSeries {
                fingerprint: api_prod,
                labels: api_prod_labels.clone(),
            },
            TenantSeries {
                fingerprint: api_stage,
                labels: api_stage_labels.clone(),
            },
            TenantSeries {
                fingerprint: worker_prod,
                labels: worker_prod_labels.clone(),
            },
        ];
        expected_tenant_a_series.sort_by_key(|series| series.fingerprint);

        assert2::assert!(index.labels_for("tenant-a", api_prod).cloned() == Some(api_prod_labels));
        assert2::assert!(index.labels_for("tenant-b", api_prod).cloned() == None);
        assert2::assert!(
            index.labels_for("tenant-b", other_tenant).cloned() == Some(other_tenant_labels)
        );
        assert2::assert!(
            index.label_names("tenant-a")
                == BTreeSet::from(["env".into(), "region".into(), "service".into()])
        );
        assert2::assert!(index.label_names("missing") == BTreeSet::new());
        assert2::assert!(
            index.label_values("tenant-a", "service")
                == BTreeSet::from(["api".into(), "worker".into()])
        );
        assert2::assert!(
            index.label_values("tenant-b", "service") == BTreeSet::from(["api".into()])
        );
        assert2::assert!(index.label_values("tenant-a", "missing") == BTreeSet::new());
        assert2::assert!(index.tenant_series("tenant-a") == expected_tenant_a_series);

        let exact_api_prod = [
            LabelPredicate::new("service", MatchOp::Equal, "api").unwrap(),
            LabelPredicate::new("env", MatchOp::Equal, "prod").unwrap(),
        ];
        let exact_and_residual = [
            LabelPredicate::new("service", MatchOp::Equal, "api").unwrap(),
            LabelPredicate::new("env", MatchOp::NotEqual, "prod").unwrap(),
            LabelPredicate::new("region", MatchOp::RegexEqual, "west|central").unwrap(),
        ];
        let no_exact_predicates = [
            LabelPredicate::new("service", MatchOp::RegexEqual, "api|worker").unwrap(),
            LabelPredicate::new("env", MatchOp::RegexNotEqual, "prod").unwrap(),
        ];
        let missing_exact = [LabelPredicate::new("service", MatchOp::Equal, "admin").unwrap()];
        let match_cases = [
            SeriesMatchCase {
                tenant: "tenant-a",
                predicates: exact_api_prod.as_slice(),
                expected: BTreeSet::from([api_prod]),
            },
            SeriesMatchCase {
                tenant: "tenant-a",
                predicates: exact_and_residual.as_slice(),
                expected: BTreeSet::from([api_stage]),
            },
            SeriesMatchCase {
                tenant: "tenant-a",
                predicates: no_exact_predicates.as_slice(),
                expected: BTreeSet::from([api_stage]),
            },
            SeriesMatchCase {
                tenant: "tenant-a",
                predicates: missing_exact.as_slice(),
                expected: BTreeSet::new(),
            },
            SeriesMatchCase {
                tenant: "missing",
                predicates: exact_api_prod.as_slice(),
                expected: BTreeSet::new(),
            },
        ];

        for case in match_cases {
            assert2::assert!(index.match_series(case.tenant, case.predicates) == case.expected);
        }
    }

    #[test]
    fn time_ranges_and_block_keys_pin_boundary_semantics_and_paths() {
        let first = TimeRange::new(10, 20).unwrap();
        let key = BlockKey::new("tenant-a", 3, 42, 47, first);
        let root = Path::new("/tmp/log-blocks");
        let prefix = ObjectPath::from("observability/logs");
        let object_key = "tenant=tenant-a/partition=3/offsets=42-47/time=10-20.parquet";
        let overlap_cases = [
            TimeRangeOverlapCase {
                range: TimeRange::new(20, 30).unwrap(),
                expected: true,
            },
            TimeRangeOverlapCase {
                range: TimeRange::new(0, 9).unwrap(),
                expected: false,
            },
            TimeRangeOverlapCase {
                range: TimeRange::new(21, 30).unwrap(),
                expected: false,
            },
        ];

        for case in overlap_cases {
            assert2::assert!(first.overlaps(case.range) == case.expected);
        }
        check!(TimeRange::new(21, 20).is_err());
        assert2::assert!(key.object_key() == object_key.to_string());
        assert2::assert!(block_path(root, &key) == root.join(object_key));
        assert2::assert!(
            log_block_object_path(&prefix, &key).to_string()
                == format!("observability/logs/{object_key}")
        );
    }

    #[test]
    fn log_block_round_trips_rows_metadata_and_rejects_out_of_range_rows() {
        let dir = tempfile::tempdir().unwrap();
        let api = series_fingerprint(&labels([crate::labels::LabelPair::new("service", "api")]));
        let worker = series_fingerprint(&labels([crate::labels::LabelPair::new(
            "service", "worker",
        )]));
        let key = BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(100, 199).unwrap());
        let rows = vec![
            LogRow::new(
                worker,
                150,
                "worker ok",
                metadata([crate::labels::LabelPair::new("pod", "worker-0")]),
            ),
            LogRow::new(
                api,
                100,
                "api start",
                metadata([
                    crate::labels::LabelPair::new("pod", "api-0"),
                    crate::labels::LabelPair::new("trace_id", "abc"),
                ]),
            ),
            LogRow::new(api, 199, "api stop", StructuredMetadata::new()),
        ];

        let descriptor = write_log_block(dir.path(), &key, rows.clone()).unwrap();
        let loaded_rows = read_log_block(dir.path(), &key).unwrap();

        check!(
            (descriptor.key.clone(), descriptor.fingerprints.clone())
                == (key.clone(), BTreeSet::from([api, worker]))
        );
        check!(descriptor.size > ByteSize::ZERO);
        check!(
            loaded_rows
                == vec![
                    LogRow::new(
                        api,
                        100,
                        "api start",
                        metadata([
                            crate::labels::LabelPair::new("pod", "api-0"),
                            crate::labels::LabelPair::new("trace_id", "abc")
                        ]),
                    ),
                    LogRow::new(api, 199, "api stop", StructuredMetadata::new()),
                    LogRow::new(
                        worker,
                        150,
                        "worker ok",
                        metadata([crate::labels::LabelPair::new("pod", "worker-0")])
                    ),
                ]
        );

        for case in [
            OutOfRangeTimestamp {
                description: "below range",
                timestamp_ns: 99,
            },
            OutOfRangeTimestamp {
                description: "above range",
                timestamp_ns: 200,
            },
        ] {
            let rows = vec![LogRow::new(
                api,
                case.timestamp_ns,
                "out of range",
                StructuredMetadata::new(),
            )];
            check!(
                matches!(
                    write_log_block(dir.path(), &key, rows),
                    Err(BlockStoreError::RowOutsideBlockTimeRange { timestamp_ns: actual, .. })
                        if actual == case.timestamp_ns
                ),
                "case {}",
                case.description
            );
        }
    }

    #[tokio::test]
    async fn object_store_log_block_round_trips_rows() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let prefix = ObjectPath::from("observability");
        let api = series_fingerprint(&labels([crate::labels::LabelPair::new("service", "api")]));
        let key = BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(100, 199).unwrap());

        let descriptor = write_log_block_to_object_store(
            &store,
            &prefix,
            &key,
            vec![LogRow::new(
                api,
                150,
                "api ok",
                metadata([crate::labels::LabelPair::new("pod", "api-0")]),
            )],
        )
        .await
        .unwrap();
        let loaded_rows = read_log_block_from_object_store(&store, &prefix, &key)
            .await
            .unwrap();

        check!(descriptor.size > ByteSize::ZERO);
        check!(
            loaded_rows
                == vec![LogRow::new(
                    api,
                    150,
                    "api ok",
                    metadata([crate::labels::LabelPair::new("pod", "api-0")])
                )]
        );
    }

    #[tokio::test]
    async fn object_store_log_index_manifests_round_trip_and_filter_by_tenant() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let prefix = ObjectPath::from("observability");
        let fixture = log_index_fixture();

        write_log_index_manifest_to_object_store(
            &store,
            &prefix,
            &fixture.labels_index,
            &fixture.block_index,
        )
        .await
        .unwrap();
        let loaded_indexes = read_log_index_manifest_from_object_store(&store, &prefix)
            .await
            .unwrap();
        check!(
            loaded_indexes
                == LogIndexes {
                    labels: fixture.labels_index.clone(),
                    blocks: fixture.block_index.clone(),
                }
        );

        write_tenant_log_index_manifest_to_object_store(
            &store,
            &prefix,
            "tenant-a",
            &fixture.labels_index,
            &fixture.block_index,
        )
        .await
        .unwrap();
        let tenant_indexes =
            read_tenant_log_index_manifest_from_object_store(&store, &prefix, "tenant-a")
                .await
                .unwrap();
        check!(
            tenant_indexes
                == LogIndexes {
                    labels: expected_tenant_a_label_index(),
                    blocks: block_index_from([fixture.first, fixture.second]),
                }
        );
    }

    #[tokio::test]
    async fn object_store_log_index_shards_are_listed_filtered_and_merged() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let prefix = ObjectPath::from("observability");
        let fixture = log_index_fixture();

        write_tenant_log_index_shards_to_object_store(
            &store,
            &prefix,
            "tenant-a",
            &[
                TimeRange::new(100, 199).unwrap(),
                TimeRange::new(200, 299).unwrap(),
                TimeRange::new(400, 499).unwrap(),
                TimeRange::new(200, 299).unwrap(),
            ],
            &fixture.labels_index,
            &fixture.block_index,
        )
        .await
        .unwrap();
        let shard_ranges =
            read_tenant_log_index_shard_ranges_from_object_store(&store, &prefix, "tenant-a")
                .await
                .unwrap();
        check!(
            shard_ranges
                == vec![
                    TimeRange::new(100, 199).unwrap(),
                    TimeRange::new(200, 299).unwrap(),
                    TimeRange::new(400, 499).unwrap(),
                ]
        );
        check!(
            list_tenant_log_index_shard_ranges_from_object_store(&store, &prefix, "tenant-a")
                .await
                .unwrap()
                == shard_ranges
        );
        let listed_overlap =
            list_tenant_log_index_shard_ranges_overlapping_query_from_object_store(
                &store,
                &prefix,
                "tenant-a",
                TimeRange::new(250, 350).unwrap(),
            )
            .await
            .unwrap();
        check!(listed_overlap == vec![TimeRange::new(200, 299).unwrap()]);

        let shard_indexes = read_tenant_log_index_shards_from_object_store(
            &store,
            &prefix,
            "tenant-a",
            TimeRange::new(150, 250).unwrap(),
        )
        .await
        .unwrap();
        check!(
            shard_indexes
                == LogIndexes {
                    labels: expected_tenant_a_label_index(),
                    blocks: block_index_from([fixture.first, fixture.second]),
                }
        );
    }

    #[test]
    fn datafusion_provider_reports_filter_pushdown_and_planned_blocks() {
        let block = BlockDescriptor::new(
            BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(100, 199).unwrap()),
            BTreeSet::from([7]),
        );
        let provider = LogBlockTableProvider::try_new_object_store(
            Arc::new(LocalFileSystem::new()) as Arc<dyn ObjectStore>,
            &ObjectPath::from("logs"),
            std::slice::from_ref(&block),
        )
        .unwrap();
        let timestamp_filter = col("timestamp_ns").gt_eq(lit(100_i64));
        let fingerprint_filter = col("series_fingerprint").eq(lit(7_u64));
        let line_filter = col("line").eq(lit("api ok"));
        let metadata_filter = col("structured_metadata").eq(lit("api ok"));
        let literal_filter = lit(true);
        let filter_cases = [
            FilterPushdownCase {
                filter: &timestamp_filter,
                expected: TableProviderFilterPushDown::Inexact,
            },
            FilterPushdownCase {
                filter: &fingerprint_filter,
                expected: TableProviderFilterPushDown::Inexact,
            },
            FilterPushdownCase {
                filter: &line_filter,
                expected: TableProviderFilterPushDown::Inexact,
            },
            FilterPushdownCase {
                filter: &metadata_filter,
                expected: TableProviderFilterPushDown::Unsupported,
            },
            FilterPushdownCase {
                filter: &literal_filter,
                expected: TableProviderFilterPushDown::Unsupported,
            },
        ];

        check!(provider.planned_blocks() == std::slice::from_ref(&block));
        for case in filter_cases {
            assert2::assert!(
                provider.supports_filters_pushdown(&[case.filter]).unwrap() == vec![case.expected]
            );
        }
        check!(
            LogBlockTableProvider::try_new_object_store(
                Arc::new(LocalFileSystem::new()) as Arc<dyn ObjectStore>,
                &ObjectPath::from("logs"),
                &[],
            )
            .is_err()
        );
    }

    #[test]
    fn block_index_replaces_sorts_and_filters_blocks() {
        let first = BlockDescriptor::new_with_size(
            BlockKey::new("tenant-a", 0, 20, 29, TimeRange::new(200, 299).unwrap()),
            BTreeSet::from([2]),
            bytes(10),
        );
        let second = BlockDescriptor::new_with_size(
            BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(100, 199).unwrap()),
            BTreeSet::from([1]),
            bytes(20),
        );
        let replacement_second =
            BlockDescriptor::new_with_size(second.key.clone(), BTreeSet::from([1, 3]), bytes(30));
        let other_tenant = BlockDescriptor::new(
            BlockKey::new("tenant-b", 0, 10, 19, TimeRange::new(100, 199).unwrap()),
            BTreeSet::from([1]),
        );
        let mut index = BlockIndex::default();

        index.insert(first.clone());
        index.insert(second);
        index.insert(other_tenant.clone());
        index.insert(replacement_second.clone());

        let expected_all = vec![replacement_second.clone(), first.clone(), other_tenant];
        let match_cases = [
            BlockMatchCase {
                name: "replacement fingerprint",
                tenant: "tenant-a",
                time_range: TimeRange::new(150, 250).unwrap(),
                fingerprints: &[1],
                expected: vec![replacement_second.clone()],
            },
            BlockMatchCase {
                name: "first fingerprint",
                tenant: "tenant-a",
                time_range: TimeRange::new(150, 250).unwrap(),
                fingerprints: &[2],
                expected: vec![first.clone()],
            },
            BlockMatchCase {
                name: "all tenant blocks",
                tenant: "tenant-a",
                time_range: TimeRange::new(150, 250).unwrap(),
                fingerprints: &[],
                expected: vec![replacement_second, first],
            },
            BlockMatchCase {
                name: "missing tenant",
                tenant: "tenant-c",
                time_range: TimeRange::new(150, 250).unwrap(),
                fingerprints: &[1],
                expected: vec![],
            },
            BlockMatchCase {
                name: "outside time range",
                tenant: "tenant-a",
                time_range: TimeRange::new(300, 400).unwrap(),
                fingerprints: &[],
                expected: vec![],
            },
            BlockMatchCase {
                name: "missing fingerprint",
                tenant: "tenant-a",
                time_range: TimeRange::new(150, 250).unwrap(),
                fingerprints: &[99],
                expected: vec![],
            },
        ];

        check!(index.blocks() == expected_all.as_slice());
        for case in match_cases {
            check!(
                index.match_blocks(case.tenant, case.time_range, case.fingerprints)
                    == case.expected,
                "case {}",
                case.name
            );
        }
    }

    #[test]
    fn manifest_json_pins_block_size_as_a_whole_byte_integer() {
        // The manifest is the on-disk log index format. `BlockDescriptor::size`
        // is a `ByteSize` in memory, but it must still serialise as exactly the
        // `size_bytes` integer it always did — and read back to the same
        // quantity.
        let fingerprint =
            series_fingerprint(&labels([crate::labels::LabelPair::new("service", "api")]));
        let manifest = LogIndexManifest {
            format_version: LOG_INDEX_MANIFEST_VERSION,
            series: vec![ManifestSeries {
                tenant: "tenant-a".to_string(),
                fingerprint,
                labels: labels([crate::labels::LabelPair::new("service", "api")]),
            }],
            blocks: vec![BlockDescriptor::new_with_size(
                BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(100, 199).unwrap()),
                BTreeSet::from([fingerprint]),
                kibibytes(3),
            )],
        };
        let expected = format!(
            concat!(
                r#"{{"format_version":1,"#,
                r#""series":[{{"tenant":"tenant-a","fingerprint":{fingerprint},"#,
                r#""labels":{{"service":"api"}}}}],"#,
                r#""blocks":[{{"key":{{"tenant":"tenant-a","partition":0,"#,
                r#""first_offset":10,"last_offset":19,"#,
                r#""time_range":{{"start_ns":100,"end_ns":199}}}},"#,
                r#""fingerprints":[{fingerprint}],"size_bytes":3072}}]}}"#,
            ),
            fingerprint = fingerprint
        );

        let encoded = serde_json::to_string(&manifest).unwrap();
        check!(encoded == expected);

        let decoded: LogIndexManifest = serde_json::from_str(&encoded).unwrap();
        check!(decoded.blocks == manifest.blocks);
        check!(decoded.blocks[0].size == kibibytes(3));
    }

    #[test]
    fn manifest_conversions_filter_validate_versions_and_fingerprints() {
        let fixture = log_index_fixture();
        let expected_tenant_labels = expected_tenant_a_label_index();
        let expected_tenant_blocks =
            block_index_from([fixture.first.clone(), fixture.second.clone()]);
        let api = series_fingerprint(&labels([crate::labels::LabelPair::new("service", "api")]));

        let full = LogIndexManifest::from_indexes(&fixture.labels_index, &fixture.block_index);
        let full_indexes = full.into_indexes().unwrap();
        check!(
            full_indexes
                == LogIndexes {
                    labels: fixture.labels_index.clone(),
                    blocks: fixture.block_index.clone(),
                }
        );

        let tenant_manifest = LogIndexManifest::from_indexes_for_tenant(
            "tenant-a",
            &fixture.labels_index,
            &fixture.block_index,
        );
        let tenant_indexes = tenant_manifest.into_indexes_for_tenant("tenant-a").unwrap();
        check!(
            tenant_indexes
                == LogIndexes {
                    labels: expected_tenant_labels,
                    blocks: expected_tenant_blocks
                }
        );

        let shard_manifest = LogIndexManifest::from_indexes_for_tenant_shard(
            "tenant-a",
            TimeRange::new(150, 250).unwrap(),
            &fixture.labels_index,
            &fixture.block_index,
        );
        let shard_indexes = shard_manifest.into_indexes_for_tenant("tenant-a").unwrap();
        check!(
            shard_indexes
                == LogIndexes {
                    labels: expected_tenant_a_label_index(),
                    blocks: block_index_from([fixture.first, fixture.second]),
                }
        );

        let bad_version = LogIndexManifest {
            format_version: LOG_INDEX_MANIFEST_VERSION + 1,
            series: Vec::new(),
            blocks: Vec::new(),
        };
        check!(matches!(
            bad_version.into_indexes(),
            Err(BlockStoreError::InvalidManifestVersion { .. })
        ));
        let bad_fingerprint = LogIndexManifest {
            format_version: LOG_INDEX_MANIFEST_VERSION,
            series: vec![ManifestSeries {
                tenant: "tenant-a".to_string(),
                fingerprint: api + 1,
                labels: labels([crate::labels::LabelPair::new("service", "api")]),
            }],
            blocks: Vec::new(),
        };
        check!(matches!(
            bad_fingerprint.into_indexes(),
            Err(BlockStoreError::ManifestFingerprintMismatch { .. })
        ));
    }

    #[test]
    fn shard_catalog_and_path_helpers_sort_parse_and_validate_ranges() {
        let prefix = ObjectPath::from("observability");
        let shard_prefix = log_tenant_index_shards_object_prefix(&prefix, "tenant-a");
        let first = TimeRange::new(-10, 20).unwrap();
        let second = TimeRange::new(30, 40).unwrap();
        let catalog = LogIndexShardCatalog::new(&[second, first, second]);

        check!(catalog.into_shards().unwrap() == vec![first, second]);
        check!(matches!(
            (LogIndexShardCatalog {
                format_version: LOG_INDEX_MANIFEST_VERSION + 1,
                shards: vec![first],
            })
            .into_shards(),
            Err(BlockStoreError::InvalidManifestVersion { .. })
        ));
        let path_cases = [
            PathExpectation {
                actual: log_index_manifest_object_path(&prefix),
                expected: "observability/index/logs/manifest.json",
            },
            PathExpectation {
                actual: log_tenant_index_shard_catalog_object_path(&prefix, "tenant-a"),
                expected: "observability/tenant=tenant-a/index/logs/shards/manifest.json",
            },
            PathExpectation {
                actual: log_tenant_index_shard_manifest_object_path(&prefix, "tenant-a", first),
                expected: "observability/tenant=tenant-a/index/logs/shards/time=-10-20/manifest.json",
            },
            PathExpectation {
                actual: log_tenant_index_shard_list_offset_object_path(
                    &prefix,
                    "tenant-a",
                    TimeRange::new(100, 199).unwrap(),
                ),
                expected: "observability/tenant=tenant-a/index/logs/shards/time=1",
            },
        ];

        for case in path_cases {
            assert2::assert!(case.actual.to_string() == case.expected);
        }
        check!(
            log_tenant_index_shard_list_offset_start_ns(TimeRange::new(100, 100).unwrap()) == 99
        );
        let parse_cases = [
            ShardParseCase {
                name: "valid shard manifest",
                location: shard_prefix
                    .clone()
                    .join("time=-10-20")
                    .join("manifest")
                    .join("snapshots")
                    .join("00000000000000000000.json"),
                expected: Some(first),
            },
            ShardParseCase {
                name: "reversed time range",
                location: shard_prefix
                    .clone()
                    .join("time=20-10")
                    .join("manifest")
                    .join("snapshots")
                    .join("00000000000000000000.json"),
                expected: None,
            },
            ShardParseCase {
                name: "wrong file name",
                location: shard_prefix.clone().join("time=10-20").join("data.json"),
                expected: None,
            },
            ShardParseCase {
                name: "extra path component",
                location: shard_prefix
                    .clone()
                    .join("time=10-20")
                    .join("manifest")
                    .join("snapshots")
                    .join("00000000000000000000.json")
                    .join("extra"),
                expected: None,
            },
            ShardParseCase {
                name: "wrong prefix",
                location: ObjectPath::from(
                    "other/time=10-20/manifest/snapshots/00000000000000000000.json",
                ),
                expected: None,
            },
        ];

        for case in parse_cases {
            check!(
                parse_log_tenant_index_shard_range_from_object_path(&shard_prefix, &case.location)
                    == case.expected,
                "case {}",
                case.name
            );
        }
    }

    /// The object-store scan reads a fraction of the block, not all of it.
    ///
    /// This is the whole of the change and the only assertion that can see it:
    /// both the old scan and the new one return the same rows, so a row count
    /// separates nothing. The old scan `get`s every block whole and decodes
    /// every row before the projection or the predicate is applied, so its
    /// byte count is the block's size exactly. A real Parquet scan fetches the
    /// footer and the column chunks the query projects, and here the query
    /// projects the narrowest of four columns while `structured_metadata`
    /// holds almost all of the bytes.
    #[tokio::test]
    async fn object_store_scan_reads_far_less_than_the_whole_block() {
        /// Rows in the fixture block.
        ///
        /// Large enough that the Parquet footer -- which the reader prefetches
        /// at a fixed size, whatever the block holds -- is a small part of it,
        /// so what the ratio below measures is the column data.
        const BLOCK_ROWS: i64 = 40_000;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let counted = Arc::new(CountingObjectStore::new(Arc::clone(&store)));
        let prefix = ObjectPath::from("observability");
        let key = BlockKey::new(
            "tenant-a",
            0,
            0,
            BLOCK_ROWS - 1,
            TimeRange::new(0, BLOCK_ROWS).unwrap(),
        );
        let descriptor = write_log_block_to_object_store(
            store.as_ref(),
            &prefix,
            &key,
            wide_metadata_rows(BLOCK_ROWS),
        )
        .await
        .unwrap();
        let block_bytes = descriptor.size.bytes_u64();

        let ctx = SessionContext::new();
        register_log_blocks_from_object_store(
            &ctx,
            "logs",
            Arc::clone(&counted) as Arc<dyn ObjectStore>,
            &prefix,
            std::slice::from_ref(&descriptor),
        )
        .unwrap();
        let batches = ctx
            .sql("select timestamp_ns from logs where timestamp_ns = 4242")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let matched: usize = batches.iter().map(RecordBatch::num_rows).sum();
        let read_bytes = counted.bytes_read();
        check!(matched == 1);
        check!(
            read_bytes * 4 < block_bytes,
            "scan read {read_bytes} of the block's {block_bytes} bytes"
        );
    }

    /// The query's predicate reaches the Parquet scan, and nothing re-applies
    /// it above.
    ///
    /// `supports_filters_pushdown` answers `Inexact` for the three scalar
    /// columns, which is a claim that the scan *may* use a predicate and the
    /// caller must not assume it did. Under the old object-store source that
    /// claim was empty: the predicate landed on a `MemTable` built from every
    /// row of every planned block, so it pruned no I/O at all. The plan is
    /// where the difference is visible -- the scan is a Parquet
    /// `DataSourceExec` carrying the predicate, and `DataFusion` has dropped
    /// the `FilterExec` the `Inexact` answer put there because the scan took
    /// the predicate over exactly.
    #[tokio::test]
    async fn object_store_scan_carries_the_predicate_into_the_parquet_source() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = ObjectPath::from("observability");
        let key = BlockKey::new("tenant-a", 0, 0, 999, TimeRange::new(0, 9_999).unwrap());
        let descriptor = write_log_block_to_object_store(
            store.as_ref(),
            &prefix,
            &key,
            wide_metadata_rows(1_000),
        )
        .await
        .unwrap();
        let fingerprint =
            series_fingerprint(&labels([crate::labels::LabelPair::new("service", "api")]));

        let ctx = SessionContext::new();
        register_log_blocks_from_object_store(
            &ctx,
            "logs",
            store,
            &prefix,
            std::slice::from_ref(&descriptor),
        )
        .unwrap();
        let plan = ctx
            .sql(&format!(
                "select series_fingerprint, timestamp_ns, line, structured_metadata \
                 from logs \
                 where timestamp_ns >= 100 and timestamp_ns <= 199 \
                 and series_fingerprint in ({fingerprint}) \
                 order by series_fingerprint, timestamp_ns"
            ))
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let rendered = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(true)
            .to_string();

        check!(
            rendered.contains("file_type=parquet"),
            "plan was:\n{rendered}"
        );
        check!(
            rendered.contains("predicate=timestamp_ns@1 >= 100"),
            "plan was:\n{rendered}"
        );
        check!(
            rendered.contains(&format!("series_fingerprint@0 = {fingerprint}")),
            "plan was:\n{rendered}"
        );
        check!(!rendered.contains("FilterExec"), "plan was:\n{rendered}");
    }

    /// A block larger than the cap is an error, not an out-of-memory kill.
    ///
    /// A tiny cap stands in for the production one so the test need not write
    /// a gibibyte; a cap at exactly the block's size is the other side of the
    /// same boundary, and it reads.
    #[tokio::test]
    async fn object_store_scan_rejects_a_block_over_the_read_cap() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = ObjectPath::from("observability");
        let key = BlockKey::new("tenant-a", 0, 0, 9, TimeRange::new(0, 99).unwrap());
        let descriptor =
            write_log_block_to_object_store(store.as_ref(), &prefix, &key, wide_metadata_rows(10))
                .await
                .unwrap();

        let over_cap = scan_with_block_read_max(
            Arc::clone(&store),
            &prefix,
            &descriptor,
            ByteSize::from_bytes(1),
        )
        .await;
        let at_cap = scan_with_block_read_max(store, &prefix, &descriptor, descriptor.size)
            .await
            .unwrap();

        check!(over_cap.is_err());
        check!(at_cap == 10);
    }

    /// The two sources answer the same question the same way.
    ///
    /// The local source has always been a `ListingTable`. The object-store
    /// source is one now, and the rows it returns for the query the querier
    /// actually issues -- projection, time bounds, fingerprint set and all --
    /// have to be the rows the local source returns for it.
    #[tokio::test]
    async fn object_store_and_local_sources_return_the_same_rows() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = ObjectPath::from("observability");
        let key = BlockKey::new("tenant-a", 0, 0, 999, TimeRange::new(0, 9_999).unwrap());
        let rows = wide_metadata_rows(1_000);
        let local = write_log_block(dir.path(), &key, rows.clone()).unwrap();
        let remote = write_log_block_to_object_store(store.as_ref(), &prefix, &key, rows)
            .await
            .unwrap();
        let query = "select series_fingerprint, timestamp_ns, line, structured_metadata \
                     from logs where timestamp_ns >= 100 and timestamp_ns <= 199 \
                     order by series_fingerprint, timestamp_ns";

        let local_ctx = SessionContext::new();
        register_log_blocks(&local_ctx, "logs", dir.path(), std::slice::from_ref(&local)).unwrap();
        let local_rows = collected_log_rows(&local_ctx, query).await;

        let remote_ctx = SessionContext::new();
        register_log_blocks_from_object_store(
            &remote_ctx,
            "logs",
            store,
            &prefix,
            std::slice::from_ref(&remote),
        )
        .unwrap();
        let remote_rows = collected_log_rows(&remote_ctx, query).await;

        check!(local_rows.len() == 100);
        check!(remote_rows == local_rows);
    }

    /// Rows whose `structured_metadata` is most of their bytes.
    ///
    /// The metadata values are distinct and hash-derived, so Parquet cannot
    /// dictionary-encode them away and the column stays the bulk of the block.
    /// That is what makes "how many bytes did the scan read" a question with
    /// two different answers.
    fn wide_metadata_rows(count: i64) -> Vec<LogRow> {
        let fingerprint =
            series_fingerprint(&labels([crate::labels::LabelPair::new("service", "api")]));
        (0..count)
            .map(|row| {
                let mut payload = String::new();
                for part in 0..8 {
                    write!(payload, "{:016x}", xxh3_64(&(row * 8 + part).to_le_bytes()))
                        .expect("writing to a String cannot fail");
                }
                LogRow::new(
                    fingerprint,
                    row,
                    format!("line {row}"),
                    metadata([crate::labels::LabelPair::new("payload", payload.as_str())]),
                )
            })
            .collect()
    }

    /// Every row `query` returns, as the `LogRow`s the columns encode.
    async fn collected_log_rows(ctx: &SessionContext, query: &str) -> Vec<LogRow> {
        ctx.sql(query)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .iter()
            .flat_map(|batch| batch_to_rows(batch).unwrap())
            .collect()
    }

    /// Rows read back through a provider capped at `block_read_max`.
    async fn scan_with_block_read_max(
        store: Arc<dyn ObjectStore>,
        prefix: &ObjectPath,
        block: &BlockDescriptor,
        block_read_max: ByteSize,
    ) -> Result<usize, DataFusionError> {
        let provider = LogBlockTableProvider::try_new_object_store_with_block_read_max(
            store,
            prefix,
            std::slice::from_ref(block),
            block_read_max,
        )
        .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("logs", Arc::new(provider))?;
        let batches = ctx.sql("select * from logs").await?.collect().await?;
        Ok(batches.iter().map(RecordBatch::num_rows).sum())
    }

    /// An object store that records how many bytes its reads fetched.
    #[derive(Debug)]
    struct CountingObjectStore {
        inner: Arc<dyn ObjectStore>,
        bytes_read: AtomicU64,
    }

    impl CountingObjectStore {
        fn new(inner: Arc<dyn ObjectStore>) -> Self {
            Self {
                inner,
                bytes_read: AtomicU64::new(0),
            }
        }

        fn bytes_read(&self) -> u64 {
            self.bytes_read.load(Ordering::Relaxed)
        }
    }

    impl std::fmt::Display for CountingObjectStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(formatter, "CountingObjectStore({})", self.inner)
        }
    }

    crate::delegate_object_store! {
        CountingObjectStore => inner;
        forward [put_opts, put_multipart_opts, list, list_with_delimiter, copy_opts, delete_stream];

        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            // A `head` is a `get_opts` that transfers no payload, and the scan
            // makes one per block on purpose. Counting its nominal range as
            // bytes read would drown out the measurement.
            let is_head = options.head;
            let result = self.inner.get_opts(location, options).await?;
            if !is_head {
                self.bytes_read
                    .fetch_add(result.range.end - result.range.start, Ordering::Relaxed);
            }
            Ok(result)
        }
    }

    struct LogIndexFixture {
        labels_index: LabelIndex,
        block_index: BlockIndex,
        first: BlockDescriptor,
        second: BlockDescriptor,
    }

    fn log_index_fixture() -> LogIndexFixture {
        let mut labels_index = LabelIndex::default();
        let api = labels_index.insert_series(
            "tenant-a",
            labels([crate::labels::LabelPair::new("service", "api")]),
        );
        let worker = labels_index.insert_series(
            "tenant-a",
            labels([crate::labels::LabelPair::new("service", "worker")]),
        );
        let other = labels_index.insert_series(
            "tenant-b",
            labels([crate::labels::LabelPair::new("service", "api")]),
        );
        let first = BlockDescriptor::new(
            BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(100, 199).unwrap()),
            BTreeSet::from([api]),
        );
        let second = BlockDescriptor::new(
            BlockKey::new("tenant-a", 0, 20, 29, TimeRange::new(200, 299).unwrap()),
            BTreeSet::from([worker]),
        );
        let other_block = BlockDescriptor::new(
            BlockKey::new("tenant-b", 0, 10, 19, TimeRange::new(100, 199).unwrap()),
            BTreeSet::from([other]),
        );
        let mut block_index = BlockIndex::default();
        block_index.insert(first.clone());
        block_index.insert(second.clone());
        block_index.insert(other_block);

        LogIndexFixture {
            labels_index,
            block_index,
            first,
            second,
        }
    }

    fn expected_tenant_a_label_index() -> LabelIndex {
        let mut index = LabelIndex::default();
        index.insert_series(
            "tenant-a",
            labels([crate::labels::LabelPair::new("service", "api")]),
        );
        index.insert_series(
            "tenant-a",
            labels([crate::labels::LabelPair::new("service", "worker")]),
        );
        index
    }

    fn block_index_from<const N: usize>(blocks: [BlockDescriptor; N]) -> BlockIndex {
        let mut index = BlockIndex::default();
        for block in blocks {
            index.insert(block);
        }
        index
    }

    fn metadata<const N: usize>(items: [crate::labels::LabelPair; N]) -> StructuredMetadata {
        let mut metadata = StructuredMetadata::new();
        for pair in items {
            metadata.insert(pair.name, pair.value);
        }
        metadata
    }

    /// A tenant reaches these keys from an untrusted header, and the keys are
    /// what separates one tenant's blocks and index shards from another's.
    #[test]
    fn a_tenant_never_widens_a_log_key_past_its_own_segment() {
        let prefix = ObjectPath::from("observability/logs");

        for case in AWKWARD_TENANTS {
            let name = case.name;
            let tenant = case.tenant;
            let key = BlockKey::new(case.tenant, 3, 42, 47, TimeRange::new(10, 20).unwrap());
            let object_key = key.object_key();
            let segments = object_key.split('/').collect::<Vec<_>>();
            assert2::check!(segments.len() == 4, "{name}");
            assert2::check!(
                !segments
                    .iter()
                    .any(|segment| matches!(*segment, "." | ".." | "")),
                "{name}"
            );
            assert2::check!(
                log_block_object_path(&prefix, &key).to_string()
                    == format!("observability/logs/{object_key}"),
                "{name}"
            );

            let shards = log_tenant_index_shards_object_prefix(&prefix, case.tenant);
            let manifest = log_tenant_index_manifest_object_path(&prefix, case.tenant);
            for path in [&shards, &manifest] {
                let parts = path
                    .prefix_match(&prefix)
                    .expect("a tenant path sits under the prefix it was given")
                    .collect::<Vec<_>>();
                assert2::check!(parts[0].as_ref().starts_with("tenant="), "{name}");
                assert2::check!(
                    parts[0]
                        .as_ref()
                        .strip_prefix("tenant=")
                        .and_then(crate::unescape_object_path_segment)
                        == Some(case.tenant.to_string()),
                    "{name}"
                );
            }
            assert2::check!(
                shards.to_string()
                    == format!(
                        "observability/logs/tenant={}/index/logs/shards",
                        crate::escape_object_path_segment(case.tenant)
                    ),
                "{name}"
            );
        }
    }

    /// `block_path` joins a multi-segment key onto a local directory, so a
    /// tenant is the one part of it an attacker chooses.
    #[test]
    fn a_block_path_stays_under_its_root() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("store");
        fs::create_dir_all(&root).unwrap();
        let resolved_root = root.canonicalize().unwrap();

        for case in AWKWARD_TENANTS {
            let name = case.name;
            let key = BlockKey::new(case.tenant, 0, 0, 1, TimeRange::new(0, 1).unwrap());
            let path = block_path(&root, &key);
            fs::create_dir_all(path.parent().expect("a block path has a parent")).unwrap();
            fs::write(&path, b"block").unwrap();

            // The resolved path, not the spelling: `..` and a leading `/` only
            // show up once the operating system walks the path.
            let resolved = path.canonicalize().unwrap();
            assert2::check!(resolved.starts_with(&resolved_root), "{name}");
            assert2::check!(resolved != resolved_root, "{name}");
        }
    }
}

mod anchored_regex_pattern;
mod append_len_prefixed;
mod batch_to_rows;
mod block_descriptor;
mod block_index;
mod block_key;
mod block_path;
mod block_store_error;
mod collect_tenant_log_index_shard_ranges;
mod delete_tenant_log_index_shard_from_object_store;
mod encode_log_block;
mod filter_references_only_pushdown_columns;
mod head_log_block_within_cap;
mod head_log_blocks_within_cap;
mod label_index;
mod label_predicate;
mod labels;
mod list_tenant_log_index_shard_ranges_from_object_store;
mod list_tenant_log_index_shard_ranges_overlapping_query_from_object_store;
mod log_block_object_path;
mod log_block_object_store_url;
mod log_block_schema;
mod log_block_table_provider;
mod log_block_table_source;
mod log_index_manifest;
mod log_index_manifest_object_path;
mod log_index_manifest_path;
mod log_index_manifest_relative_path;
mod log_index_manifest_version;
mod log_index_shard_catalog;
mod log_listing_options;
mod log_row;
mod log_tenant_index_manifest_object_path;
mod log_tenant_index_shard_catalog_object_path;
mod log_tenant_index_shard_list_offset_object_path;
mod log_tenant_index_shard_list_offset_start_ns;
mod log_tenant_index_shard_manifest_object_path;
mod log_tenant_index_shards_object_prefix;
mod manifest_series;
mod match_op;
mod object_store_log_listing_table;
mod parse_log_tenant_index_shard_range_from_object_path;
mod planned_log_listing_table;
mod read_log_block;
mod read_log_block_from_object_store;
mod read_log_block_from_reader;
mod read_log_index_manifest;
mod read_log_index_manifest_from_object_store;
mod read_tenant_log_index_manifest_from_object_store;
mod read_tenant_log_index_shard_from_object_store;
mod read_tenant_log_index_shard_ranges_from_object_store;
mod read_tenant_log_index_shard_ranges_or_empty_from_object_store;
mod read_tenant_log_index_shards_from_object_store;
mod register_log_blocks;
mod register_log_blocks_from_object_store;
mod rows_to_batch;
mod series_fingerprint;
mod structured_metadata;
mod structured_metadata_array;
mod structured_metadata_type;
mod structured_metadata_value;
mod time_range;
mod validate_planned_blocks;
mod validate_rows;
mod write_log_block;
mod write_log_block_to_object_store;
mod write_log_index_manifest;
mod write_log_index_manifest_to_object_store;
mod write_tenant_log_index_manifest_to_object_store;
mod write_tenant_log_index_shard_catalog_to_object_store;
mod write_tenant_log_index_shard_to_object_store;
mod write_tenant_log_index_shards_to_object_store;

use anchored_regex_pattern::anchored_regex_pattern;
use append_len_prefixed::append_len_prefixed;
use batch_to_rows::batch_to_rows;
pub use block_descriptor::BlockDescriptor;
pub use block_index::BlockIndex;
pub use block_key::BlockKey;
pub use block_path::block_path;
pub use block_store_error::BlockStoreError;
use collect_tenant_log_index_shard_ranges::collect_tenant_log_index_shard_ranges;
pub use delete_tenant_log_index_shard_from_object_store::delete_tenant_log_index_shard_from_object_store;
use encode_log_block::encode_log_block;
use filter_references_only_pushdown_columns::filter_references_only_pushdown_columns;
use head_log_block_within_cap::head_log_block_within_cap;
use head_log_blocks_within_cap::head_log_blocks_within_cap;
pub use label_index::{LabelIndex, TenantSeries};
pub use label_predicate::LabelPredicate;
pub use labels::{Labels, labels};
pub use list_tenant_log_index_shard_ranges_from_object_store::list_tenant_log_index_shard_ranges_from_object_store;
pub use list_tenant_log_index_shard_ranges_overlapping_query_from_object_store::list_tenant_log_index_shard_ranges_overlapping_query_from_object_store;
pub use log_block_object_path::log_block_object_path;
use log_block_object_store_url::next_log_block_object_store_url;
use log_block_schema::log_block_schema;
pub use log_block_table_provider::LogBlockTableProvider;
use log_block_table_source::LogBlockTableSource;
use log_index_manifest::LogIndexManifest;
pub use log_index_manifest::LogIndexes;
pub use log_index_manifest_object_path::log_index_manifest_object_path;
pub use log_index_manifest_path::log_index_manifest_path;
use log_index_manifest_relative_path::LOG_INDEX_MANIFEST_RELATIVE_PATH;
use log_index_manifest_version::LOG_INDEX_MANIFEST_VERSION;
use log_index_shard_catalog::LogIndexShardCatalog;
use log_listing_options::log_listing_options;
pub use log_row::LogRow;
pub use log_tenant_index_manifest_object_path::log_tenant_index_manifest_object_path;
pub use log_tenant_index_shard_catalog_object_path::log_tenant_index_shard_catalog_object_path;
pub use log_tenant_index_shard_list_offset_object_path::log_tenant_index_shard_list_offset_object_path;
pub use log_tenant_index_shard_list_offset_start_ns::log_tenant_index_shard_list_offset_start_ns;
pub use log_tenant_index_shard_manifest_object_path::log_tenant_index_shard_manifest_object_path;
pub use log_tenant_index_shards_object_prefix::log_tenant_index_shards_object_prefix;
use manifest_series::ManifestSeries;
pub use match_op::MatchOp;
use object_store_log_listing_table::object_store_log_listing_table;
use parse_log_tenant_index_shard_range_from_object_path::parse_log_tenant_index_shard_range_from_object_path;
use planned_log_listing_table::planned_log_listing_table;
pub use read_log_block::read_log_block;
pub use read_log_block_from_object_store::read_log_block_from_object_store;
use read_log_block_from_reader::read_log_block_from_reader;
pub use read_log_index_manifest::read_log_index_manifest;
pub use read_log_index_manifest_from_object_store::read_log_index_manifest_from_object_store;
pub use read_tenant_log_index_manifest_from_object_store::read_tenant_log_index_manifest_from_object_store;
pub use read_tenant_log_index_shard_from_object_store::read_tenant_log_index_shard_from_object_store;
use read_tenant_log_index_shard_from_object_store::{
    log_snapshot_error, read_log_index_shard_snapshot_base,
};
pub use read_tenant_log_index_shard_ranges_from_object_store::read_tenant_log_index_shard_ranges_from_object_store;
pub use read_tenant_log_index_shard_ranges_or_empty_from_object_store::read_tenant_log_index_shard_ranges_or_empty_from_object_store;
pub use read_tenant_log_index_shards_from_object_store::read_tenant_log_index_shards_from_object_store;
pub use register_log_blocks::register_log_blocks;
pub use register_log_blocks_from_object_store::register_log_blocks_from_object_store;
use rows_to_batch::rows_to_batch;
pub use series_fingerprint::{SeriesFingerprint, series_fingerprint};
pub use structured_metadata::StructuredMetadata;
use structured_metadata_array::structured_metadata_array;
use structured_metadata_type::structured_metadata_type;
use structured_metadata_value::structured_metadata_value;
pub use time_range::TimeRange;
use validate_planned_blocks::validate_planned_blocks;
use validate_rows::validate_rows;
pub use write_log_block::write_log_block;
pub use write_log_block_to_object_store::write_log_block_to_object_store;
pub use write_log_index_manifest::write_log_index_manifest;
pub use write_log_index_manifest_to_object_store::write_log_index_manifest_to_object_store;
pub use write_tenant_log_index_manifest_to_object_store::write_tenant_log_index_manifest_to_object_store;
pub use write_tenant_log_index_shard_catalog_to_object_store::write_tenant_log_index_shard_catalog_to_object_store;
pub use write_tenant_log_index_shard_to_object_store::{
    update_tenant_log_index_shard_to_object_store, write_tenant_log_index_shard_to_object_store,
};
pub use write_tenant_log_index_shards_to_object_store::write_tenant_log_index_shards_to_object_store;
