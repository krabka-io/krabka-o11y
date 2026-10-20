use std::{ops::Range, time::Instant};

use assert2::assert;
use bytes::Bytes;
use futures::{FutureExt, future::BoxFuture};
use krabka_blockstore::{ProfileSampleFields, ProfileSampleRow, encode_profile_samples};
use krabka_pprof::{EngineOpts, FlameEngine, ProfileStore};
use object_store::ObjectStoreExt;
use parquet::{
    arrow::{
        ArrowWriter,
        arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder},
    },
    errors::Result as ParquetResult,
    file::{
        metadata::ParquetMetaData,
        properties::{EnabledStatistics, WriterProperties},
    },
};

use super::*;
use crate::{
    cold_store::ColdProfileStore,
    compactor::compact_blocks,
    test_support::{CpuRecord, build_test_block, cpu_record, index_with_series},
};

const ROWS_PER_GROUP: usize = 2048;
const GROUPS: usize = 16;
const PROFILE_TYPE: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
const SELECTED_FP: u64 = u64::MAX - 8;

async fn measured_reader(
    bytes: Bytes,
    fetched: Arc<AtomicUsize>,
    metadata: Option<Arc<ParquetMetaData>>,
) -> CachedObjectReader {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let path = Path::from("profiles.parquet");
    store.put(&path, bytes.into()).await.unwrap();
    let mut inner = ObjectReader::new(store, path);
    inner.fetched_bytes = fetched;
    CachedObjectReader { inner, metadata }
}

fn fixture(statistics: EnabledStatistics) -> Bytes {
    let props = WriterProperties::builder()
        .set_statistics_enabled(statistics)
        .set_max_row_group_row_count(Some(ROWS_PER_GROUP))
        .build();
    let mut data = Vec::new();
    let mut writer = ArrowWriter::try_new(
        &mut data,
        krabka_blockstore::profile_samples_schema(),
        Some(props),
    )
    .unwrap();
    for group in 0..GROUPS {
        let rows = (0..ROWS_PER_GROUP)
            .map(|row| {
                let ordinal = group * ROWS_PER_GROUP + row;
                ProfileSampleRow {
                    timestamp: i64::try_from(group * 10_000 + row).unwrap(),
                    fields: ProfileSampleFields {
                        series_fingerprint: if group == 8 {
                            SELECTED_FP
                        } else {
                            group as u64
                        },
                        profile_type: if row % 2 == 0 { PROFILE_TYPE } else { "other" }.to_string(),
                        stacktrace_id: ordinal as u64,
                        value: i64::try_from(ordinal).unwrap(),
                        stacktrace_partition: if row % 2 == 0 { 9_u64 << 32 } else { 3 },
                        total_value: 1,
                        span_id: Some(ordinal as u64),
                        trace_id: Some((ordinal as u64).to_le_bytes().repeat(16)),
                        wal_sample_ids: vec![(ordinal as u64).to_le_bytes().repeat(32)],
                    },
                }
            })
            .collect::<Vec<_>>();
        writer
            .write(&encode_profile_samples(&rows).unwrap())
            .unwrap();
        writer.flush().unwrap();
    }
    writer.close().unwrap();
    Bytes::from(data)
}

fn filter(start_ms: i64, end_ms: i64, metadata: bool) -> ScanFilter {
    ScanFilter {
        fingerprints: BTreeSet::from([SELECTED_FP]),
        profile_type: (!metadata).then(|| PROFILE_TYPE.to_string()),
        start_ms,
        end_ms,
    }
}

fn partitions() -> BTreeMap<u64, u64> {
    BTreeMap::from([(9_u64 << 32, 1_u64 << 32), (3, (1_u64 << 32) + 1)])
}

fn normalized(batches: &[RecordBatch], metadata: bool) -> RecordBatch {
    let batches = batches
        .iter()
        .map(|batch| {
            let batch = if metadata {
                batch.project(&[0, 1]).unwrap()
            } else {
                batch.clone()
            };
            if metadata {
                return batch;
            }
            let mut columns = batch.columns().to_vec();
            columns[2] = arrow::compute::cast(&columns[2], &DataType::Utf8).unwrap();
            let mut fields = batch.schema().fields().to_vec();
            fields[2] = Arc::new(arrow::datatypes::Field::new(
                "profile_type",
                DataType::Utf8,
                false,
            ));
            RecordBatch::try_new(Arc::new(arrow::datatypes::Schema::new(fields)), columns).unwrap()
        })
        .collect::<Vec<_>>();
    arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap()
}

#[tokio::test]
async fn selective_range_reads_prune_groups_and_project_metadata() {
    let data = fixture(EnabledStatistics::Chunk);
    let baseline = ParquetRecordBatchReaderBuilder::try_new(data.clone()).unwrap();
    let cached_metadata = Arc::clone(baseline.metadata());
    let all_batches = baseline
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let expected = all_batches
        .iter()
        .map(|batch| {
            filter_and_remap_batch(
                batch,
                &partitions(),
                BlockRowFilter {
                    fps: &BTreeSet::from([SELECTED_FP]),
                    profile_type: PROFILE_TYPE,
                    start_ms: 80_000,
                    end_ms: 80_002,
                },
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let mut measurements = Vec::new();
    for metadata in [false, true] {
        for warm in [false, true] {
            let fetched = Arc::new(AtomicUsize::new(0));
            let started = Instant::now();
            let output = scan(
                measured_reader(
                    data.clone(),
                    Arc::clone(&fetched),
                    warm.then(|| Arc::clone(&cached_metadata)),
                )
                .await,
                filter(80_000, 80_002, metadata),
                (!metadata).then(partitions),
            )
            .await
            .unwrap();
            let bytes = fetched.load(Ordering::Relaxed);
            let rows = output
                .batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>();
            assert!(rows == if metadata { 3 } else { 2 });
            assert!(output.predicate_rows == ROWS_PER_GROUP);
            assert!(bytes < data.len() / 4);
            if metadata {
                assert!(output.batches.iter().all(|batch| batch.num_columns() == 2));
                let actual = normalized(&output.batches, true);
                assert!(
                    actual
                        .column(1)
                        .as_primitive::<Int64Type>()
                        .values()
                        .as_ref()
                        == [80_000, 80_001, 80_002]
                );
            } else {
                assert!(normalized(&output.batches, false) == normalized(&expected, false));
            }
            measurements.push(bytes);
            eprintln!(
                "metadata={metadata} warm={warm} bytes={bytes}/{} predicate_rows={}/{} output_rows={rows} latency={:?}",
                data.len(),
                output.predicate_rows,
                GROUPS * ROWS_PER_GROUP,
                started.elapsed()
            );
        }
    }
    assert!(measurements[1] < measurements[0]);
    assert!(measurements[2] < measurements[0]);
    assert!(measurements[3] < measurements[1]);
}

#[tokio::test]
async fn absent_statistics_keep_rows_and_inclusive_time_boundaries() {
    let data = fixture(EnabledStatistics::None);
    for (start, end, expected) in [
        (80_000, 80_002, 2),
        (80_002, 80_002, 1),
        (79_999, 80_000, 1),
        (80_001, 80_001, 0),
        (90_000, 80_000, 0),
    ] {
        let output = scan(
            measured_reader(data.clone(), Arc::default(), None).await,
            filter(start, end, false),
            Some(partitions()),
        )
        .await
        .unwrap();
        assert!(
            output
                .batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>()
                == expected
        );
    }
}

#[tokio::test]
async fn fingerprint_and_timestamp_statistics_prune_independently() {
    let data = fixture(EnabledStatistics::Chunk);
    for time_only in [false, true] {
        let mut selected = filter(0, i64::MAX, true);
        if time_only {
            selected.fingerprints.extend(0..GROUPS as u64);
            selected.start_ms = 80_000;
            selected.end_ms = 80_002;
        }
        let output = scan(
            measured_reader(data.clone(), Arc::default(), None).await,
            selected,
            None,
        )
        .await
        .unwrap();
        assert!(output.predicate_rows == ROWS_PER_GROUP);
        assert!(
            output
                .batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>()
                == if time_only { 3 } else { ROWS_PER_GROUP }
        );
    }
}

struct CachedObjectReader {
    inner: ObjectReader,
    metadata: Option<Arc<ParquetMetaData>>,
}

impl AsyncFileReader for CachedObjectReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        self.inner.get_bytes(range)
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        self.inner.get_byte_ranges(ranges)
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        async move {
            if let Some(metadata) = &self.metadata {
                return Ok(Arc::clone(metadata));
            }
            self.inner.get_metadata(options).await
        }
        .boxed()
    }
}

// Run each mode in a separate process with /usr/bin/time -v to measure peak RSS.
#[tokio::test]
#[ignore = "manual latency and peak-memory measurement"]
async fn scan_resource_usage() {
    let fixture_path = std::env::var("PROFILE_SCAN_FIXTURE").expect("PROFILE_SCAN_FIXTURE");
    let mode = std::env::var("PROFILE_SCAN_MODE").unwrap_or_else(|_| "fixture".into());
    if mode == "fixture" {
        std::fs::write(fixture_path, fixture(EnabledStatistics::Chunk)).unwrap();
        return;
    }
    let warm = std::env::var("PROFILE_SCAN_WARM").is_ok_and(|value| value == "1");
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::local::LocalFileSystem::new());
    let path = Path::from_filesystem_path(&fixture_path).unwrap();
    let mut reader = ObjectReader::new(store, path);
    let metadata = if warm {
        Some(reader.get_metadata(None).await.unwrap())
    } else {
        None
    };
    let started = Instant::now();
    let rows = if mode.starts_with("full") {
        let data = Bytes::from(std::fs::read(&fixture_path).unwrap());
        let builder = if let Some(metadata) = metadata {
            ParquetRecordBatchReaderBuilder::new_with_metadata(
                data,
                ArrowReaderMetadata::try_new(metadata, ArrowReaderOptions::default()).unwrap(),
            )
        } else {
            ParquetRecordBatchReaderBuilder::try_new(data).unwrap()
        };
        let mut count = 0;
        for batch in builder.build().unwrap() {
            let batch = batch.unwrap();
            if mode == "full-metadata" {
                let fingerprints = batch.column(0).as_primitive::<UInt64Type>();
                let timestamps = batch.column(1).as_primitive::<Int64Type>();
                count += (0..batch.num_rows())
                    .filter(|&row| {
                        fingerprints.value(row) == SELECTED_FP
                            && (80_000..=80_002).contains(&timestamps.value(row))
                    })
                    .count();
                continue;
            }
            let selected = filter_and_remap_batch(
                &batch,
                &partitions(),
                BlockRowFilter {
                    fps: &BTreeSet::from([SELECTED_FP]),
                    profile_type: PROFILE_TYPE,
                    start_ms: 80_000,
                    end_ms: 80_002,
                },
            )
            .unwrap();
            count += selected.num_rows();
        }
        count
    } else {
        let metadata_only = mode == "metadata";
        let output = scan(
            CachedObjectReader {
                inner: reader,
                metadata,
            },
            filter(80_000, 80_002, metadata_only),
            (!metadata_only).then(partitions),
        )
        .await
        .unwrap();
        output
            .batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>()
    };
    assert!(rows == if mode.ends_with("metadata") { 3 } else { 2 });
    eprintln!(
        "mode={mode} warm_metadata={warm} rows={rows} elapsed_us={}",
        started.elapsed().as_micros()
    );
}

#[tokio::test]
async fn compacted_queries_and_metadata_keep_time_boundaries_and_symbols() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let records = (1..=4)
        .map(|ordinal| {
            cpu_record(CpuRecord {
                tenant: "t",
                service: if ordinal % 2 == 0 { "worker" } else { "api" },
                stack: vec![0],
                value: ordinal,
                timestamp_ns: ordinal * 1_000_000_000,
                function: if ordinal < 3 { "first" } else { "second" },
            })
        })
        .collect::<Vec<_>>();
    let first = build_test_block(&store, &records[..2], 0).await;
    let second = build_test_block(&store, &records[2..], 1).await;
    let mut index = index_with_series(&records, &[&first, &second]);
    let mut before_index = index_with_series(&records, &[&first, &second]);
    for block in [&first, &second] {
        for index in [&mut index, &mut before_index] {
            index.add_profile_block(
                "t",
                &block.object_key,
                vec![crate::blockbuilder::STACKTRACE_PARTITION],
            );
        }
    }
    let before = Arc::new(ColdProfileStore::new(
        Arc::clone(&store),
        Arc::new(before_index),
    ));
    let engine = FlameEngine::new(Arc::clone(&before), EngineOpts::default());
    let ranges = [(1000, 1000), (2000, 3000), (0, i64::MAX), (1001, 1999)];
    let mut expected = Vec::new();
    for (start, end) in ranges {
        expected.push((
            before.series("t", &[], &[], start, end).await.unwrap(),
            engine
                .select_merge_stacktraces(
                    "t",
                    PROFILE_TYPE,
                    r#"{service_name="api"}"#,
                    start,
                    end,
                    0,
                )
                .await
                .unwrap(),
        ));
    }
    compact_blocks(
        &store,
        &mut index,
        "t",
        &[first.object_key, second.object_key],
        "blocks/t/compacted.parquet",
    )
    .await
    .unwrap();
    let after = Arc::new(ColdProfileStore::new(store, Arc::new(index)));
    let engine = FlameEngine::new(Arc::clone(&after), EngineOpts::default());
    for ((start, end), expected) in ranges.into_iter().zip(expected) {
        let actual = (
            after.series("t", &[], &[], start, end).await.unwrap(),
            engine
                .select_merge_stacktraces(
                    "t",
                    PROFILE_TYPE,
                    r#"{service_name="api"}"#,
                    start,
                    end,
                    0,
                )
                .await
                .unwrap(),
        );
        assert!(actual == expected);
    }
}
