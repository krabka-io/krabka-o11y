//! Block-builder helpers for WAL records -> profile sample blocks.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Instant,
};

use arrow::record_batch::RecordBatch;
use krabka_blockstore::{
    BlockIndex, BlockMeta, BlockWriter, DEFAULT_INDEX_SNAPSHOT_MAX, IndexSnapshotRetain, Labels,
    ObjectStoreMetrics, ObjectStoreOperation, ObjectStoreRetryPolicy, OffsetRange, ProfileIndex,
    ProfileSampleRow, RetryingObjectStore, SummaryColumns, encode_profile_samples,
    escape_object_path_segment, profile_samples_decl, retry_object_store,
};
use krabka_client_consumer::{AutoOffsetReset, Consumer, ConsumerRecord};
use krabka_observability::{
    ReadinessGate,
    wal_consumer_metrics::WalConsumerMetrics,
    wal_group_assignment::{WalAssignmentWatch, WalRebalanceListener},
};
use krabka_pprof::{FunctionRec, LineRec, LocationRec, MappingRec, MappingSymbolization, SymbolDb};
use krabka_units::{
    ByteSize, Time, convert::StdDurationExt as _, kibibytes, mebibytes, millis, secs,
};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use tokio_util::sync::CancellationToken;
use tracing::Instrument as _;

use crate::{
    error::ProfilesError,
    metrics::ServiceMetrics,
    wal::{PROFILES_WAL_TOPIC, ProfileRecord, WalMapping, WalSymbolSet},
};

pub(crate) fn wal_sample_ids(
    position: Option<crate::wal::WalPosition>,
    ordinal: usize,
) -> Result<Vec<Vec<u8>>, ProfilesError> {
    let Some(position) = position else {
        return Ok(Vec::new());
    };
    let ordinal =
        u64::try_from(ordinal).map_err(|error| ProfilesError::Decode(error.to_string()))?;
    Ok(vec![position.sample_identity(ordinal)])
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::{assert, check};
    use bytes::Bytes;
    use krabka_client_consumer::ConsumerRecord;
    use krabka_pprof::{MappingSymbolizationParts, SymbolDb};
    use krabka_units::{convert::TimeExt as _, minutes};
    use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory};

    use super::*;
    use crate::wal::{ProfileRecord, WalMapping, WalSample, WalSymbolSet};

    struct TenantSegmentCase {
        name: &'static str,
        tenant: &'static str,
        segment: &'static str,
    }

    struct TenantRowCount {
        tenant: &'static str,
        row_count: usize,
    }

    struct WalHandoffCase {
        name: &'static str,
        topic: &'static str,
        offset: i64,
        value: i64,
        expected: i64,
    }

    fn format_headers() -> Vec<krabka_client_consumer::Header> {
        vec![krabka_client_consumer::Header {
            key: krabka_observability::persisted_format::PERSISTED_FORMAT_HEADER.to_string(),
            value: Some(Bytes::from_static(
                krabka_observability::persisted_format::PERSISTED_FORMAT_VERSION,
            )),
        }]
    }

    fn rec(name: &str, value: i64) -> ProfileRecord {
        ProfileRecord {
            tenant: "t".into(),
            labels: vec![
                krabka_blockstore::LabelPair::new("__name__", name),
                krabka_blockstore::LabelPair::new("service_name", "api"),
                krabka_blockstore::LabelPair::new(
                    "__profile_type__",
                    "process_cpu:cpu:nanoseconds:cpu:nanoseconds",
                ),
            ],
            profile_type: "process_cpu:cpu:nanoseconds:cpu:nanoseconds".into(),
            samples: vec![WalSample {
                stacktrace_location_refs: vec![0, 1],
                value,
                timestamp_ns: 1_700_000_000_000_000_000,
                span_id: None,
                trace_id: None,
            }],
            symbols: WalSymbolSet {
                strings: vec![String::new(), "a".into(), "b".into()],
                functions: vec![],
                locations: vec![],
                mappings: vec![],
            },
        }
    }

    #[test]
    fn mapping_rec_maps_each_flag_from_its_own_source_field() {
        // Each flag distinct so a wrong source assignment (e.g. all from
        // has_functions) is caught.
        let mapping = WalMapping {
            memory_start: 0x1000,
            memory_limit: 0x2000,
            file_offset: 0x10,
            filename: 1,
            build_id: 2,
            has_functions: true.into(),
            has_filenames: false.into(),
            has_line_numbers: true.into(),
            has_inline_frames: false.into(),
        };
        let strings = [0_u32, 10, 20];

        let rec = mapping_rec(&mapping, &strings);

        assert!(
            rec == MappingRec {
                memory_start: 0x1000,
                memory_limit: 0x2000,
                file_offset: 0x10,
                filename: 10,
                build_id: 20,
                symbolization: MappingSymbolization::from_parts(MappingSymbolizationParts {
                    functions: true,
                    filenames: false,
                    line_numbers: true,
                    inline_frames: false,
                }),
            }
        );

        // And the inverse pattern, to ensure no field is hard-wired.
        let inverted = WalMapping {
            has_functions: false.into(),
            has_filenames: true.into(),
            has_line_numbers: false.into(),
            has_inline_frames: true.into(),
            ..mapping
        };
        let rec = mapping_rec(&inverted, &strings);
        assert!(
            rec == MappingRec {
                memory_start: 0x1000,
                memory_limit: 0x2000,
                file_offset: 0x10,
                filename: 10,
                build_id: 20,
                symbolization: MappingSymbolization::from_parts(MappingSymbolizationParts {
                    functions: false,
                    filenames: true,
                    line_numbers: false,
                    inline_frames: true,
                }),
            }
        );
    }

    #[test]
    fn object_key_is_deterministic() {
        let a = object_key("t", 0, 10, 20, 100, 200);
        let b = object_key("t", 0, 10, 20, 100, 200);
        let c = object_key("t", 0, 10, 21, 100, 200);

        assert!(a == b);
        assert!(a != c);
    }

    // A tenant id may hold `*`, `!`, `'`, `(` and `)`. Each one becomes one
    // escaped segment that the object store keeps as it is, and the unescape
    // gives back the tenant the WAL record named.
    #[test]
    fn the_tenant_is_one_escaped_segment_that_reads_back_as_the_tenant() {
        let cases = [
            TenantSegmentCase {
                name: "plain",
                tenant: "tenant-a",
                segment: "tenant-a",
            },
            TenantSegmentCase {
                name: "star",
                tenant: "a*b",
                segment: "a!2Ab",
            },
            TenantSegmentCase {
                name: "escape marker",
                tenant: "a!b",
                segment: "a!21b",
            },
            TenantSegmentCase {
                name: "quote and parentheses",
                tenant: "a'(b)",
                segment: "a!27!28b!29",
            },
            TenantSegmentCase {
                name: "dots",
                tenant: "a.b",
                segment: "a.b",
            },
        ];

        for case in cases {
            let key = object_key(case.tenant, 3, 10, 20, 100, 200);
            check!(
                key == format!(
                    "blocks/{}/00003/00000000000000000010-00000000000000000020-100-200.parquet",
                    case.segment
                ),
                "{}",
                case.name
            );
            let path = Path::from(key.as_str());
            check!(
                path.as_ref() == key,
                "{}: the object store keeps the key",
                case.name
            );
            let parts: Vec<_> = path.parts().collect();
            check!(parts.len() == 4, "{}: {key}", case.name);
            check!(
                parts
                    .get(1)
                    .and_then(|part| krabka_blockstore::unescape_object_path_segment(
                        part.as_ref()
                    ))
                    == Some(case.tenant.to_string()),
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn block_builder_snapshot_policy_preserves_defaults() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let config = BlockBuilderConfig::new("broker:9092".into(), store);

        check!(config.index_snapshot_max == krabka_blockstore::DEFAULT_INDEX_SNAPSHOT_MAX);
        check!(
            config.index_snapshot_retain.into_value()
                == krabka_blockstore::DEFAULT_INDEX_SNAPSHOT_RETAIN
        );
    }

    #[test]
    fn block_builder_wal_fetch_limits_preserve_defaults() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let config = BlockBuilderConfig::new("broker:9092".into(), store);

        check!(config.wal_fetch_max == DEFAULT_WAL_FETCH_MAX);
        check!(config.wal_fetch_partition_max == DEFAULT_WAL_FETCH_PARTITION_MAX);
    }

    #[test]
    fn intern_record_dedups_identical_stacks() {
        let mut symdb = SymbolDb::default();
        let r = rec("cpu", 5);

        let ids1 = intern_record(&mut symdb, &r).unwrap();
        let ids2 = intern_record(&mut symdb, &r).unwrap();

        assert!(ids1 == ids2);
    }

    #[test]
    fn samples_batch_matches_profile_schema() {
        let batch = samples_batch(&[BuiltSample {
            timestamp_ns: 100,
            fields: krabka_blockstore::ProfileSampleFields {
                series_fingerprint: 1,
                profile_type: "process_cpu:cpu:nanoseconds:cpu:nanoseconds".into(),
                stacktrace_id: 7,
                value: 5,
                stacktrace_partition: 0,
                total_value: 5,
                span_id: None,
                trace_id: None,
                wal_sample_ids: Vec::new(),
            },
        }])
        .unwrap();

        assert!(batch.schema() == krabka_blockstore::profile_samples_schema());
        assert!(batch.num_rows() == 1);
    }

    #[tokio::test]
    async fn build_block_writes_samples_and_symdb() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let records = vec![rec("cpu", 5), rec("cpu", 7)];

        let metas = build_block(
            &store,
            "t",
            0,
            &records,
            OffsetRange::new(10, 20),
            &krabka_blockstore::ObjectStoreMetrics::unregistered(),
        )
        .await
        .unwrap();

        assert!(metas.len() == 1);
        check!(metas[0].tenant == "t");
        check!(metas[0].row_count == 2);
        check!(metas[0].min_ts == 1_700_000_000_000);
        check!(metas[0].max_ts == 1_700_000_000_000);
        let symdb_key = format!("{}.symdb", metas[0].object_key);
        assert!(
            store
                .head(&object_store::path::Path::from(symdb_key))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn flush_consumer_records_groups_by_tenant_and_partition() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut index = ProfileIndex::new();
        let mut tenant_b = rec("cpu", 11);
        tenant_b.tenant = "u".to_string();
        let records = vec![
            consumer_record(0, 10, rec("cpu", 5)),
            consumer_record(0, 11, rec("cpu", 7)),
            consumer_record(1, 3, tenant_b),
        ];

        let metas = flush_consumer_records_with_index(
            &store,
            &mut index,
            &records,
            100,
            &krabka_blockstore::ObjectStoreMetrics::unregistered(),
        )
        .await
        .unwrap();

        check!(metas.len() == 2);
        for expected in [
            TenantRowCount {
                tenant: "t",
                row_count: 2,
            },
            TenantRowCount {
                tenant: "u",
                row_count: 1,
            },
        ] {
            check!(
                metas
                    .iter()
                    .any(|meta| meta.tenant == expected.tenant
                        && meta.row_count == expected.row_count)
            );
        }
        for meta in metas {
            assert!(
                store
                    .head(&object_store::path::Path::from(meta.object_key))
                    .await
                    .is_ok()
            );
        }
        check!(index.profile_types("t") == vec!["process_cpu:cpu:nanoseconds:cpu:nanoseconds"]);
        for tenant in ["t", "u"] {
            check!(BlockIndex::block_count(&index, tenant) == 1);
        }
    }

    #[tokio::test]
    async fn handoff_distinguishes_reused_wal_positions() {
        use krabka_pprof::{EngineOpts, FlameEngine, UnionProfileStore};

        let record = |value| {
            let mut record = rec("cpu", value);
            record.samples[0].stacktrace_location_refs = vec![0];
            record.symbols.functions = vec![crate::wal::WalFunction {
                name: 1,
                system_name: 1,
                filename: 2,
                start_line: 1,
            }];
            record.symbols.locations = vec![crate::wal::WalLocation {
                address: 0x40,
                mapping_id: 0,
                lines: vec![crate::wal::WalLine {
                    function_id: 0,
                    line: 11,
                }],
            }];
            record
        };
        let profile_type = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
        for case in [
            WalHandoffCase {
                name: "same WAL record",
                topic: PROFILES_WAL_TOPIC,
                offset: 10,
                value: 7,
                expected: 7,
            },
            WalHandoffCase {
                name: "different topic",
                topic: "replacement-profiles-wal",
                offset: 10,
                value: 7,
                expected: 14,
            },
            WalHandoffCase {
                name: "reused offset with different data",
                topic: PROFILES_WAL_TOPIC,
                offset: 10,
                value: 11,
                expected: 18,
            },
            WalHandoffCase {
                name: "independent identical record",
                topic: PROFILES_WAL_TOPIC,
                offset: 11,
                value: 7,
                expected: 14,
            },
        ] {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let mut index = ProfileIndex::new();
            let stored = consumer_record(0, 10, record(7));
            flush_consumer_records_with_index(
                &store,
                &mut index,
                &[stored],
                100,
                &ObjectStoreMetrics::unregistered(),
            )
            .await
            .unwrap();

            let mut incoming = consumer_record(0, case.offset, record(case.value));
            incoming.topic = case.topic.to_string();
            let bytes = incoming.value.as_deref().unwrap();
            let hot = crate::hot_store::WalTailProfileStore::new();
            hot.append_records_with_positions([crate::hot_store::PositionedWalRecord::new(
                ProfileRecord::decode(bytes).unwrap(),
                Some(crate::wal::WalPosition::from_record(&incoming, bytes)),
            )])
            .unwrap();
            let cold = crate::cold_store::ColdProfileStore::new(store, Arc::new(index));
            let union = UnionProfileStore::new(Arc::new(hot), Arc::new(cold));
            let engine = FlameEngine::new(Arc::new(union), EngineOpts::default());
            let flamegraph = engine
                .select_merge_stacktraces(
                    "t",
                    profile_type,
                    r#"{service_name="api"}"#,
                    0,
                    i64::MAX,
                    0,
                )
                .await
                .unwrap();
            check!(flamegraph.total == case.expected, "{}", case.name);
        }
    }

    #[test]
    fn accumulator_flushes_on_record_threshold() {
        let mut accumulator = ConsumerRecordAccumulator::new(2, minutes(1));
        let start = Instant::now();

        accumulator.push(vec![consumer_record(0, 10, rec("cpu", 5))], start);
        assert!(!accumulator.should_flush(start));

        accumulator.push(
            vec![consumer_record(0, 11, rec("cpu", 7))],
            start + millis(1).to_std(),
        );
        check!(accumulator.should_flush(start + millis(1).to_std()));
        check!(accumulator.take().len() == 2);
        check!(!accumulator.should_flush(start + minutes(2).to_std()));
    }

    #[test]
    fn accumulator_flushes_on_max_age() {
        let mut accumulator = ConsumerRecordAccumulator::new(100, secs(10));
        let start = Instant::now();

        accumulator.push(vec![consumer_record(0, 10, rec("cpu", 5))], start);
        assert!(!accumulator.should_flush(start + secs(9).to_std()));
        assert!(accumulator.should_flush(start + secs(10).to_std()));
    }

    fn consumer_record(partition: i32, offset: i64, record: ProfileRecord) -> ConsumerRecord {
        let value = Bytes::from(record.encode().unwrap());
        drop(record);
        ConsumerRecord {
            topic: PROFILES_WAL_TOPIC.to_string(),
            partition,
            offset,
            leader_epoch: -1,
            timestamp: 0,
            timestamp_type: krabka_client_consumer::TimestampType::CreateTime,
            key: None,
            value: Some(value),
            headers: format_headers(),
        }
    }
}

mod block_builder_config;
mod build_block;
mod consumer_record_accumulator;
mod record_writer;
mod run_with_config;

pub use block_builder_config::BlockBuilderConfig;
pub use build_block::build_block;
pub(crate) use build_block::{PositionedProfileRecord, build_block_with_positions};
use consumer_record_accumulator::ConsumerRecordAccumulator;
pub use record_writer::flush_consumer_records_with_index;
use record_writer::intern_symbols;
pub use run_with_config::run_with_config;

pub(crate) struct SymbolRefs {
    pub(crate) locations: Vec<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, derive_more::Deref)]
pub struct BuiltSample {
    pub timestamp_ns: i64,
    #[deref]
    pub fields: krabka_blockstore::ProfileSampleFields,
}

/// The object-store prefix every profile block sits under.
///
/// Both writers of a block key start here: [`object_key`]
/// for a block the block-builder flushes, and the compactor for a block a
/// merge writes. The orphan sweep lists this prefix and deletes what the index
/// does not name, so a block written outside it would never be reclaimed, and
/// anything else written inside it would be deleted.
pub const BLOCK_OBJECT_PREFIX: &str = "blocks";

/// Scale between the epoch-nanosecond timestamps that the WAL carries and the
/// epoch-millisecond timestamps that index blocks.
///
/// This is instant arithmetic and not an extent, so it stays exact integer
/// division. An absolute nanosecond timestamp is about 1.8e18 and cannot
/// round-trip through the `f64` seconds that a `Time` stores.
pub(crate) const NANOS_PER_MILLI: i64 = 1_000_000;

pub const DEFAULT_FLUSH_MAX_AGE: Time = secs(10);

pub const DEFAULT_FLUSH_RECORDS: usize = 1024;

pub const DEFAULT_WAL_FETCH_MAX: ByteSize = mebibytes(2);

pub const DEFAULT_WAL_FETCH_PARTITION_MAX: ByteSize = kibibytes(256);

pub const STACKTRACE_PARTITION: u64 = 0;

#[must_use]
pub fn profile_timestamp_ms(timestamp_ns: i64) -> i64 {
    timestamp_ns.div_euclid(NANOS_PER_MILLI)
}

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub async fn flush_consumer_records(
    store: &Arc<dyn ObjectStore>,
    records: &[ConsumerRecord],
    flush_records: usize,
    metrics: &ObjectStoreMetrics,
) -> Result<Vec<BlockMeta>, ProfilesError> {
    let mut index = ProfileIndex::new();
    flush_consumer_records_with_index(store, &mut index, records, flush_records, metrics).await
}

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub fn intern_record(symdb: &mut SymbolDb, rec: &ProfileRecord) -> Result<Vec<u32>, ProfilesError> {
    let symbols = intern_symbols(symdb, &rec.symbols)?;
    rec.samples
        .iter()
        .map(|sample| {
            let stack = sample
                .stacktrace_location_refs
                .iter()
                .map(|location_ref| remap_ref(*location_ref, &symbols.locations))
                .collect::<Vec<_>>();
            Ok(symdb.intern_stacktrace(STACKTRACE_PARTITION, &stack))
        })
        .collect()
}

pub(crate) fn mapping_rec(mapping: &WalMapping, strings: &[u32]) -> MappingRec {
    MappingRec {
        memory_start: mapping.memory_start,
        memory_limit: mapping.memory_limit,
        file_offset: mapping.file_offset,
        filename: remap_ref(mapping.filename, strings),
        build_id: remap_ref(mapping.build_id, strings),
        symbolization: MappingSymbolization::from_parts(krabka_pprof::MappingSymbolizationParts {
            functions: mapping.has_functions.get(),
            filenames: mapping.has_filenames.get(),
            line_numbers: mapping.has_line_numbers.get(),
            inline_frames: mapping.has_inline_frames.get(),
        }),
    }
}

/// Object key of one block that the block-builder writes.
///
/// The tenant is one path segment. It goes through
/// [`escape_object_path_segment`], so a tenant name that holds a character
/// such as `*` or `!` stays in its own segment and survives the object store.
/// The other segments come from integers and cannot hold a separator.
#[must_use]
pub fn object_key(
    tenant: &str,
    partition: i32,
    min_offset: i64,
    max_offset: i64,
    min_ts: i64,
    max_ts: i64,
) -> String {
    let tenant = escape_object_path_segment(tenant);
    format!(
        "{BLOCK_OBJECT_PREFIX}/{tenant}/{partition:05}/{min_offset:020}-{max_offset:020}-{min_ts}-{max_ts}.parquet"
    )
}

pub(crate) fn remap_ref(reference: u32, table: &[u32]) -> u32 {
    usize::try_from(reference)
        .ok()
        .and_then(|idx| table.get(idx))
        .copied()
        .or_else(|| {
            reference
                .checked_sub(1)
                .and_then(|idx| usize::try_from(idx).ok())
                .and_then(|idx| table.get(idx))
                .copied()
        })
        .unwrap_or(reference)
}

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub async fn run(shutdown: CancellationToken) -> Result<(), ProfilesError> {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    run_with_config(
        BlockBuilderConfig::new("127.0.0.1:9092".to_string(), store),
        shutdown,
    )
    .await
}

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub fn samples_batch(rows: &[BuiltSample]) -> Result<RecordBatch, ProfilesError> {
    let rows = rows
        .iter()
        .map(|row| ProfileSampleRow {
            timestamp: row.timestamp_ns,
            fields: row.fields.clone(),
        })
        .collect::<Vec<_>>();
    encode_profile_samples(&rows).map_err(|err| ProfilesError::Block(err.to_string()))
}
