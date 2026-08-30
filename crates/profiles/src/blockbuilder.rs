//! Block-builder helpers for WAL records -> profile sample blocks.

use std::{
    collections::{BTreeMap, BTreeSet},
    io::Cursor,
    sync::Arc,
    time::Instant,
};

use arrow::record_batch::RecordBatch;
use krabka_blockstore::{
    BlockIndex, BlockMeta, DEFAULT_INDEX_SNAPSHOT_MAX, IndexSnapshotRetain, Labels, ProfileIndex,
    ProfileSampleRow, encode_profile_samples,
};
use krabka_client_consumer::{AutoOffsetReset, Consumer, ConsumerRecord};
use krabka_pprof::{FunctionRec, LineRec, LocationRec, MappingRec, MappingSymbolization, SymbolDb};
use krabka_units::{
    ByteSize, Time, convert::StdDurationExt as _, kibibytes, mebibytes, millis, secs,
};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use parquet::arrow::ArrowWriter;
use tracing::Instrument as _;

use crate::{
    error::ProfilesError,
    metrics::ServiceMetrics,
    wal::{PROFILES_WAL_TOPIC, ProfileRecord, WalMapping, WalSymbolSet},
};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::{assert, check};
    use bytes::Bytes;
    use krabka_client_consumer::ConsumerRecord;
    use krabka_pprof::SymbolDb;
    use krabka_units::{convert::TimeExt as _, minutes};
    use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory};

    use super::*;
    use crate::wal::{ProfileRecord, WalMapping, WalSample, WalSymbolSet};

    fn rec(name: &str, value: i64) -> ProfileRecord {
        ProfileRecord {
            tenant: "t".into(),
            labels: vec![
                ("__name__".into(), name.into()),
                ("service_name".into(), "api".into()),
                (
                    "__profile_type__".into(),
                    "process_cpu:cpu:nanoseconds:cpu:nanoseconds".into(),
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
                symbolization: MappingSymbolization::from_parts((true, false, true, false)),
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
                symbolization: MappingSymbolization::from_parts((false, true, false, true)),
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

    #[test]
    fn block_builder_snapshot_policy_preserves_defaults() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let config = BlockBuilderConfig::new("broker:9092".into(), store);

        assert_eq!(
            config.index_snapshot_max,
            krabka_blockstore::DEFAULT_INDEX_SNAPSHOT_MAX
        );
        assert_eq!(
            config.index_snapshot_retain.into_value(),
            krabka_blockstore::DEFAULT_INDEX_SNAPSHOT_RETAIN
        );
    }

    #[test]
    fn block_builder_wal_fetch_limits_preserve_defaults() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let config = BlockBuilderConfig::new("broker:9092".into(), store);

        assert_eq!(config.wal_fetch_max, DEFAULT_WAL_FETCH_MAX);
        assert_eq!(
            config.wal_fetch_partition_max,
            DEFAULT_WAL_FETCH_PARTITION_MAX
        );
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
            series_fingerprint: 1,
            timestamp_ns: 100,
            profile_type: "process_cpu:cpu:nanoseconds:cpu:nanoseconds".into(),
            stacktrace_id: 7,
            value: 5,
            stacktrace_partition: 0,
            total_value: 5,
            span_id: None,
            trace_id: None,
        }])
        .unwrap();

        assert!(batch.schema() == krabka_blockstore::profile_samples_schema());
        assert!(batch.num_rows() == 1);
    }

    #[tokio::test]
    async fn build_block_writes_samples_and_symdb() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let records = vec![rec("cpu", 5), rec("cpu", 7)];

        let metas = build_block(&store, "t", 0, &records, (10, 20))
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

        let metas = flush_consumer_records_with_index(&store, &mut index, &records, 100)
            .await
            .unwrap();

        check!(metas.len() == 2);
        for (tenant, row_count) in [("t", 2), ("u", 1)] {
            check!(
                metas
                    .iter()
                    .any(|meta| meta.tenant == tenant && meta.row_count == row_count)
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
            key: None,
            value: Some(value),
            headers: Vec::new(),
        }
    }
}

mod block_builder_config;
mod build_block;
mod built_sample;
mod consumer_record_accumulator;
mod default_flush_max_age;
mod default_flush_records;
mod default_wal_fetch_max;
mod default_wal_fetch_partition_max;
mod flush_consumer_records;
mod flush_consumer_records_with_index;
mod intern_record;
mod intern_symbols;
mod mapping_rec;
mod nanos_per_milli;
mod object_key;
mod profile_timestamp_ms;
mod remap_ref;
mod run;
mod run_with_config;
mod samples_batch;
mod stacktrace_partition;
mod symbol_refs;

pub use block_builder_config::BlockBuilderConfig;
pub use build_block::build_block;
pub use built_sample::BuiltSample;
use consumer_record_accumulator::ConsumerRecordAccumulator;
pub use default_flush_max_age::DEFAULT_FLUSH_MAX_AGE;
pub use default_flush_records::DEFAULT_FLUSH_RECORDS;
pub use default_wal_fetch_max::DEFAULT_WAL_FETCH_MAX;
pub use default_wal_fetch_partition_max::DEFAULT_WAL_FETCH_PARTITION_MAX;
pub use flush_consumer_records::flush_consumer_records;
pub use flush_consumer_records_with_index::flush_consumer_records_with_index;
pub use intern_record::intern_record;
use intern_symbols::intern_symbols;
use mapping_rec::mapping_rec;
use nanos_per_milli::NANOS_PER_MILLI;
pub use object_key::object_key;
pub use profile_timestamp_ms::profile_timestamp_ms;
use remap_ref::remap_ref;
pub use run::run;
pub use run_with_config::run_with_config;
pub use samples_batch::samples_batch;
pub use stacktrace_partition::STACKTRACE_PARTITION;
use symbol_refs::SymbolRefs;
