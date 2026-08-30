//! Shared columnar block-store primitives for Krabka observability signals.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{self, Cursor},
    path::{Path, PathBuf},
    sync::Arc,
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
    datasource::{
        MemTable, TableProvider,
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
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
use xxhash_rust::xxh3::xxh3_64;

#[cfg(test)]
mod tests {

    /// The manifest path is where every reader looks for the log index. An
    /// empty `PathBuf` in its place points every caller at the store root.
    #[test]
    fn log_index_manifest_path_joins_the_relative_path() {
        let path = super::log_index_manifest_path("/var/lib/krabka");
        assert2::check!(path == std::path::Path::new("/var/lib/krabka/index/logs/manifest.json"));
    }
    use assert2::check;
    use datafusion::prelude::{col, lit};
    use object_store::{local::LocalFileSystem, path::Path as ObjectPath};

    use super::*;

    #[test]
    fn labels_and_fingerprints_are_canonicalized_with_length_prefixes() {
        let label_set = labels([("service", "api"), ("env", "prod")]);
        let expected = Labels::from([
            ("env".to_string(), "prod".to_string()),
            ("service".to_string(), "api".to_string()),
        ]);

        assert2::assert!(label_set == expected);
        assert2::assert!(series_fingerprint(&expected) != 0);
        assert2::assert!(
            series_fingerprint(&labels([("a", "bc")]))
                != series_fingerprint(&labels([("ab", "c")]))
        );
    }

    #[test]
    fn label_predicates_match_exact_absent_and_anchored_regex_values() {
        let label_set = labels([("service", "api"), ("env", "prod")]);
        let cases = [
            ("exact match", "service", MatchOp::Equal, "api", true),
            ("exact mismatch", "service", MatchOp::Equal, "worker", false),
            (
                "not equal match",
                "service",
                MatchOp::NotEqual,
                "worker",
                true,
            ),
            (
                "absent not equal",
                "cluster",
                MatchOp::NotEqual,
                "east",
                true,
            ),
            (
                "anchored regex match",
                "service",
                MatchOp::RegexEqual,
                "api|worker",
                true,
            ),
            (
                "negative regex match",
                "service",
                MatchOp::RegexNotEqual,
                "api-.+",
                true,
            ),
            (
                "absent negative regex",
                "cluster",
                MatchOp::RegexNotEqual,
                "east",
                true,
            ),
            (
                "anchored regex mismatch",
                "service",
                MatchOp::RegexEqual,
                "p",
                false,
            ),
        ];

        for (_case, name, op, value, expected) in cases {
            assert2::assert!(
                LabelPredicate::new(name, op, value)
                    .unwrap()
                    .matches(&label_set)
                    == expected
            );
        }
        check!(LabelPredicate::new("service", MatchOp::RegexEqual, "[").is_err());
    }

    #[test]
    fn label_index_filters_by_tenant_exact_postings_and_residual_predicates() {
        let mut index = LabelIndex::default();
        let api_prod_labels = labels([("service", "api"), ("env", "prod"), ("region", "east")]);
        let api_stage_labels = labels([("service", "api"), ("env", "stage"), ("region", "west")]);
        let worker_prod_labels =
            labels([("service", "worker"), ("env", "prod"), ("region", "east")]);
        let other_tenant_labels =
            labels([("service", "api"), ("env", "prod"), ("region", "north")]);
        let api_prod = index.insert_series("tenant-a", api_prod_labels.clone());
        let api_stage = index.insert_series("tenant-a", api_stage_labels.clone());
        let worker_prod = index.insert_series("tenant-a", worker_prod_labels.clone());
        let other_tenant = index.insert_series("tenant-b", other_tenant_labels.clone());
        let mut expected_tenant_a_series = vec![
            (api_prod, api_prod_labels.clone()),
            (api_stage, api_stage_labels.clone()),
            (worker_prod, worker_prod_labels.clone()),
        ];
        expected_tenant_a_series.sort_by_key(|(fingerprint, _)| *fingerprint);

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
            (
                "exact api prod",
                "tenant-a",
                exact_api_prod.as_slice(),
                BTreeSet::from([api_prod]),
            ),
            (
                "exact and residual",
                "tenant-a",
                exact_and_residual.as_slice(),
                BTreeSet::from([api_stage]),
            ),
            (
                "residual predicates only",
                "tenant-a",
                no_exact_predicates.as_slice(),
                BTreeSet::from([api_stage]),
            ),
            (
                "missing exact value",
                "tenant-a",
                missing_exact.as_slice(),
                BTreeSet::new(),
            ),
            (
                "missing tenant",
                "missing",
                exact_api_prod.as_slice(),
                BTreeSet::new(),
            ),
        ];

        for (_name, tenant, predicates, expected) in match_cases {
            assert2::assert!(index.match_series(tenant, predicates) == expected);
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
            ("touching boundary", TimeRange::new(20, 30).unwrap(), true),
            ("strictly before", TimeRange::new(0, 9).unwrap(), false),
            ("strictly after", TimeRange::new(21, 30).unwrap(), false),
        ];

        for (_name, other, expected) in overlap_cases {
            assert2::assert!(first.overlaps(other) == expected);
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
        let api = series_fingerprint(&labels([("service", "api")]));
        let worker = series_fingerprint(&labels([("service", "worker")]));
        let key = BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(100, 199).unwrap());
        let rows = vec![
            LogRow::new(worker, 150, "worker ok", metadata([("pod", "worker-0")])),
            LogRow::new(
                api,
                100,
                "api start",
                metadata([("pod", "api-0"), ("trace_id", "abc")]),
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
                        metadata([("pod", "api-0"), ("trace_id", "abc")]),
                    ),
                    LogRow::new(api, 199, "api stop", StructuredMetadata::new()),
                    LogRow::new(worker, 150, "worker ok", metadata([("pod", "worker-0")])),
                ]
        );

        for (name, timestamp_ns) in [("below range", 99), ("above range", 200)] {
            let rows = vec![LogRow::new(
                api,
                timestamp_ns,
                "out of range",
                StructuredMetadata::new(),
            )];
            check!(
                matches!(
                    write_log_block(dir.path(), &key, rows),
                    Err(BlockStoreError::RowOutsideBlockTimeRange { timestamp_ns: actual, .. })
                        if actual == timestamp_ns
                ),
                "case {name}"
            );
        }
    }

    #[tokio::test]
    async fn object_store_log_block_round_trips_rows() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let prefix = ObjectPath::from("observability");
        let api = series_fingerprint(&labels([("service", "api")]));
        let key = BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(100, 199).unwrap());

        let descriptor = write_log_block_to_object_store(
            &store,
            &prefix,
            &key,
            vec![LogRow::new(
                api,
                150,
                "api ok",
                metadata([("pod", "api-0")]),
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
                    metadata([("pod", "api-0")])
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
        let (loaded_labels, loaded_blocks) =
            read_log_index_manifest_from_object_store(&store, &prefix)
                .await
                .unwrap();
        check!(
            (loaded_labels, loaded_blocks)
                == (fixture.labels_index.clone(), fixture.block_index.clone())
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
        let (tenant_labels, tenant_blocks) =
            read_tenant_log_index_manifest_from_object_store(&store, &prefix, "tenant-a")
                .await
                .unwrap();
        check!(
            (tenant_labels, tenant_blocks)
                == (
                    expected_tenant_a_label_index(),
                    block_index_from([fixture.first, fixture.second]),
                )
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

        let (shard_labels, shard_blocks) = read_tenant_log_index_shards_from_object_store(
            &store,
            &prefix,
            "tenant-a",
            TimeRange::new(150, 250).unwrap(),
        )
        .await
        .unwrap();
        check!(
            (shard_labels, shard_blocks)
                == (
                    expected_tenant_a_label_index(),
                    block_index_from([fixture.first, fixture.second]),
                )
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
            ObjectPath::from("logs"),
            std::slice::from_ref(&block),
        )
        .unwrap();
        let timestamp_filter = col("timestamp_ns").gt_eq(lit(100_i64));
        let fingerprint_filter = col("series_fingerprint").eq(lit(7_u64));
        let line_filter = col("line").eq(lit("api ok"));
        let metadata_filter = col("structured_metadata").eq(lit("api ok"));
        let literal_filter = lit(true);
        let filter_cases = [
            (
                "timestamp",
                &timestamp_filter,
                TableProviderFilterPushDown::Inexact,
            ),
            (
                "fingerprint",
                &fingerprint_filter,
                TableProviderFilterPushDown::Inexact,
            ),
            ("line", &line_filter, TableProviderFilterPushDown::Inexact),
            (
                "metadata",
                &metadata_filter,
                TableProviderFilterPushDown::Unsupported,
            ),
            (
                "literal",
                &literal_filter,
                TableProviderFilterPushDown::Unsupported,
            ),
        ];

        check!(provider.planned_blocks() == std::slice::from_ref(&block));
        for (_name, filter, expected) in filter_cases {
            assert2::assert!(
                provider.supports_filters_pushdown(&[filter]).unwrap() == vec![expected]
            );
        }
        check!(
            LogBlockTableProvider::try_new_object_store(
                Arc::new(LocalFileSystem::new()) as Arc<dyn ObjectStore>,
                ObjectPath::from("logs"),
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
            (
                "replacement fingerprint",
                "tenant-a",
                TimeRange::new(150, 250).unwrap(),
                &[1][..],
                vec![replacement_second.clone()],
            ),
            (
                "first fingerprint",
                "tenant-a",
                TimeRange::new(150, 250).unwrap(),
                &[2][..],
                vec![first.clone()],
            ),
            (
                "all tenant blocks",
                "tenant-a",
                TimeRange::new(150, 250).unwrap(),
                &[][..],
                vec![replacement_second, first],
            ),
            (
                "missing tenant",
                "tenant-c",
                TimeRange::new(150, 250).unwrap(),
                &[1][..],
                vec![],
            ),
            (
                "outside time range",
                "tenant-a",
                TimeRange::new(300, 400).unwrap(),
                &[][..],
                vec![],
            ),
            (
                "missing fingerprint",
                "tenant-a",
                TimeRange::new(150, 250).unwrap(),
                &[99][..],
                vec![],
            ),
        ];

        check!(index.blocks() == expected_all.as_slice());
        for (name, tenant, time_range, fingerprints, expected) in match_cases {
            check!(
                index.match_blocks(tenant, time_range, fingerprints) == expected,
                "case {name}"
            );
        }
    }

    #[test]
    fn manifest_json_pins_block_size_as_a_whole_byte_integer() {
        // The manifest is the on-disk log index format. `BlockDescriptor::size`
        // is a `ByteSize` in memory, but it must still serialise as exactly the
        // `size_bytes` integer it always did — and read back to the same
        // quantity.
        let fingerprint = series_fingerprint(&labels([("service", "api")]));
        let manifest = LogIndexManifest {
            format_version: LOG_INDEX_MANIFEST_VERSION,
            series: vec![ManifestSeries {
                tenant: "tenant-a".to_string(),
                fingerprint,
                labels: labels([("service", "api")]),
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
        let api = series_fingerprint(&labels([("service", "api")]));

        let full = LogIndexManifest::from_indexes(&fixture.labels_index, &fixture.block_index);
        let (full_labels, full_blocks) = full.into_indexes().unwrap();
        check!(
            (full_labels, full_blocks)
                == (fixture.labels_index.clone(), fixture.block_index.clone())
        );

        let tenant_manifest = LogIndexManifest::from_indexes_for_tenant(
            "tenant-a",
            &fixture.labels_index,
            &fixture.block_index,
        );
        let (tenant_labels, tenant_blocks) =
            tenant_manifest.into_indexes_for_tenant("tenant-a").unwrap();
        check!((tenant_labels, tenant_blocks) == (expected_tenant_labels, expected_tenant_blocks));

        let shard_manifest = LogIndexManifest::from_indexes_for_tenant_shard(
            "tenant-a",
            TimeRange::new(150, 250).unwrap(),
            &fixture.labels_index,
            &fixture.block_index,
        );
        let (shard_labels, shard_blocks) =
            shard_manifest.into_indexes_for_tenant("tenant-a").unwrap();
        check!(
            (shard_labels, shard_blocks)
                == (
                    expected_tenant_a_label_index(),
                    block_index_from([fixture.first, fixture.second]),
                )
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
                labels: labels([("service", "api")]),
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
            (
                "global manifest",
                log_index_manifest_object_path(&prefix),
                "observability/index/logs/manifest.json",
            ),
            (
                "tenant shard catalog",
                log_tenant_index_shard_catalog_object_path(&prefix, "tenant-a"),
                "observability/tenant=tenant-a/index/logs/shards/manifest.json",
            ),
            (
                "tenant shard manifest",
                log_tenant_index_shard_manifest_object_path(&prefix, "tenant-a", first),
                "observability/tenant=tenant-a/index/logs/shards/time=-10-20/manifest.json",
            ),
            (
                "tenant shard list offset",
                log_tenant_index_shard_list_offset_object_path(
                    &prefix,
                    "tenant-a",
                    TimeRange::new(100, 199).unwrap(),
                ),
                "observability/tenant=tenant-a/index/logs/shards/time=1",
            ),
        ];

        for (_name, actual, expected) in path_cases {
            assert2::assert!(actual.to_string() == expected);
        }
        check!(
            log_tenant_index_shard_list_offset_start_ns(TimeRange::new(100, 100).unwrap()) == 99
        );
        let parse_cases = [
            (
                "valid shard manifest",
                shard_prefix
                    .clone()
                    .join("time=-10-20")
                    .join("manifest.json"),
                Some(first),
            ),
            (
                "reversed time range",
                shard_prefix
                    .clone()
                    .join("time=20-10")
                    .join("manifest.json"),
                None,
            ),
            (
                "wrong file name",
                shard_prefix.clone().join("time=10-20").join("data.json"),
                None,
            ),
            (
                "extra path component",
                shard_prefix
                    .clone()
                    .join("time=10-20")
                    .join("manifest.json")
                    .join("extra"),
                None,
            ),
            (
                "wrong prefix",
                ObjectPath::from("other/time=10-20/manifest.json"),
                None,
            ),
        ];

        for (name, location, expected) in parse_cases {
            check!(
                parse_log_tenant_index_shard_range_from_object_path(&shard_prefix, &location)
                    == expected,
                "case {name}"
            );
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
        let api = labels_index.insert_series("tenant-a", labels([("service", "api")]));
        let worker = labels_index.insert_series("tenant-a", labels([("service", "worker")]));
        let other = labels_index.insert_series("tenant-b", labels([("service", "api")]));
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
        index.insert_series("tenant-a", labels([("service", "api")]));
        index.insert_series("tenant-a", labels([("service", "worker")]));
        index
    }

    fn block_index_from<const N: usize>(blocks: [BlockDescriptor; N]) -> BlockIndex {
        let mut index = BlockIndex::default();
        for block in blocks {
            index.insert(block);
        }
        index
    }

    fn metadata<const N: usize>(items: [(&str, &str); N]) -> StructuredMetadata {
        items
            .into_iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }
}

// === split-modules: generated submodules ===
mod anchored_regex_pattern;
mod append_len_prefixed;
mod batch_to_rows;
mod block_descriptor;
mod block_index;
mod block_key;
mod block_path;
mod block_store_error;
mod collect_tenant_log_index_shard_ranges;
mod encode_log_block;
mod filter_references_only_pushdown_columns;
mod label_index;
mod label_predicate;
mod labels;
mod list_tenant_log_index_shard_ranges_from_object_store;
mod list_tenant_log_index_shard_ranges_overlapping_query_from_object_store;
mod log_block_object_path;
mod log_block_schema;
mod log_block_table_provider;
mod log_block_table_source;
mod log_index_manifest;
mod log_index_manifest_object_path;
mod log_index_manifest_path;
mod log_index_manifest_relative_path;
mod log_index_manifest_version;
mod log_index_shard_catalog;
mod log_row;
mod log_tenant_index_manifest_object_path;
mod log_tenant_index_shard_catalog_object_path;
mod log_tenant_index_shard_list_offset_object_path;
mod log_tenant_index_shard_list_offset_start_ns;
mod log_tenant_index_shard_manifest_object_path;
mod log_tenant_index_shards_object_prefix;
mod manifest_series;
mod match_op;
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
use encode_log_block::encode_log_block;
use filter_references_only_pushdown_columns::filter_references_only_pushdown_columns;
pub use label_index::LabelIndex;
pub use label_predicate::LabelPredicate;
pub use labels::Labels;
pub use labels::labels;
pub use list_tenant_log_index_shard_ranges_from_object_store::list_tenant_log_index_shard_ranges_from_object_store;
pub use list_tenant_log_index_shard_ranges_overlapping_query_from_object_store::list_tenant_log_index_shard_ranges_overlapping_query_from_object_store;
pub use log_block_object_path::log_block_object_path;
use log_block_schema::log_block_schema;
pub use log_block_table_provider::LogBlockTableProvider;
use log_block_table_source::LogBlockTableSource;
use log_index_manifest::LogIndexManifest;
pub use log_index_manifest_object_path::log_index_manifest_object_path;
pub use log_index_manifest_path::log_index_manifest_path;
use log_index_manifest_relative_path::LOG_INDEX_MANIFEST_RELATIVE_PATH;
use log_index_manifest_version::LOG_INDEX_MANIFEST_VERSION;
use log_index_shard_catalog::LogIndexShardCatalog;
pub use log_row::LogRow;
pub use log_tenant_index_manifest_object_path::log_tenant_index_manifest_object_path;
pub use log_tenant_index_shard_catalog_object_path::log_tenant_index_shard_catalog_object_path;
pub use log_tenant_index_shard_list_offset_object_path::log_tenant_index_shard_list_offset_object_path;
pub use log_tenant_index_shard_list_offset_start_ns::log_tenant_index_shard_list_offset_start_ns;
pub use log_tenant_index_shard_manifest_object_path::log_tenant_index_shard_manifest_object_path;
pub use log_tenant_index_shards_object_prefix::log_tenant_index_shards_object_prefix;
use manifest_series::ManifestSeries;
pub use match_op::MatchOp;
use parse_log_tenant_index_shard_range_from_object_path::parse_log_tenant_index_shard_range_from_object_path;
use planned_log_listing_table::planned_log_listing_table;
pub use read_log_block::read_log_block;
pub use read_log_block_from_object_store::read_log_block_from_object_store;
use read_log_block_from_reader::read_log_block_from_reader;
pub use read_log_index_manifest::read_log_index_manifest;
pub use read_log_index_manifest_from_object_store::read_log_index_manifest_from_object_store;
pub use read_tenant_log_index_manifest_from_object_store::read_tenant_log_index_manifest_from_object_store;
pub use read_tenant_log_index_shard_from_object_store::read_tenant_log_index_shard_from_object_store;
pub use read_tenant_log_index_shard_ranges_from_object_store::read_tenant_log_index_shard_ranges_from_object_store;
pub use read_tenant_log_index_shards_from_object_store::read_tenant_log_index_shards_from_object_store;
pub use register_log_blocks::register_log_blocks;
pub use register_log_blocks_from_object_store::register_log_blocks_from_object_store;
use rows_to_batch::rows_to_batch;
pub use series_fingerprint::SeriesFingerprint;
pub use series_fingerprint::series_fingerprint;
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
pub use write_tenant_log_index_shard_to_object_store::write_tenant_log_index_shard_to_object_store;
pub use write_tenant_log_index_shards_to_object_store::write_tenant_log_index_shards_to_object_store;
