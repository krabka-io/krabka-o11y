//! Reading one log block back out of object storage, two ways.
//!
//! `row_struct_round_trip` is what the object-store log scan used to do for
//! every planned block: `get` the whole Parquet object, decode every row into
//! an owned `LogRow`, and only then apply the query's projection and
//! predicate. `parquet_scan` is what it does now: a `ListingTable` over the
//! same object, which hands `DataFusion` the projection, the predicate and the
//! limit before any column is decoded.
//!
//! The two lines are not measuring the same work, and that is the measurement.
//! Both answer the same question -- the rows of one series inside a narrow
//! time window -- and the gap between them is what the whole-block
//! materialisation cost. `row_struct_round_trip` is also the *floor* of the
//! old path rather than the whole of it: the old scan re-encoded its
//! `Vec<LogRow>` into an Arrow `RecordBatch` and scanned a `MemTable` over it,
//! and neither of those is charged here, because `rows_to_batch` is private to
//! the block store. The real gap is wider than the one this file reports.
//!
//! The sweep is in rows per block. The old path is linear in the block's rows
//! whatever the query asks for, because it decodes all of them; the new one
//! should track what the query selects instead. If the two lines ever converge
//! at the top of the sweep, the pushdown has stopped reaching the scan.

use std::{hint::black_box, sync::Arc};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::prelude::SessionContext;
use krabka_blockstore::{
    BlockKey, LogRow, TimeRange, labels, read_log_block_from_object_store,
    register_log_blocks_from_object_store, series_fingerprint, write_log_block_to_object_store,
};
use krabka_o11y_benches::Seeded;
use object_store::{ObjectStore, memory::InMemory, path::Path as ObjectPath};
use tokio::runtime::Runtime;

/// Rows per block in the sweep.
const CASES: [i64; 3] = [10_000, 100_000, 1_000_000];

/// The window the query asks for, as a fraction of the block's time range.
/// One per cent, so the predicate is selective enough that decoding the other
/// ninety-nine is visible.
const WINDOW_DIVISOR: i64 = 100;

fn log_object_store_scan(criterion: &mut Criterion) {
    let runtime = Runtime::new().expect("a tokio runtime");
    let mut group = criterion.benchmark_group("log_object_store_scan");
    group.sample_size(10);

    let prefix = ObjectPath::from("observability");
    for rows in CASES {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let key = BlockKey::new(
            "tenant-a",
            0,
            0,
            rows - 1,
            TimeRange::new(0, rows).expect("a forward time range"),
        );
        let fingerprints = SHARDS
            .iter()
            .map(|shard| series_fingerprint(&labels([("service", "api"), ("shard", *shard)])))
            .collect::<Vec<_>>();
        let wanted = fingerprints[0];
        let window_end = rows / WINDOW_DIVISOR;

        let planned = runtime.block_on(async {
            write_log_block_to_object_store(
                store.as_ref(),
                &prefix,
                &key,
                block_rows(rows, &fingerprints),
            )
            .await
            .expect("the fixture block is valid")
        });

        group.throughput(Throughput::Elements(
            u64::try_from(rows).expect("a row count fits a u64"),
        ));

        // What the scan used to do: the whole object into `Vec<LogRow>`, then
        // the query's predicate applied to the rows in memory.
        group.bench_with_input(
            BenchmarkId::new("row_struct_round_trip", rows),
            &rows,
            |bencher, _| {
                bencher.to_async(&runtime).iter(|| async {
                    let all = read_log_block_from_object_store(store.as_ref(), &prefix, &key)
                        .await
                        .expect("the block reads back");
                    let matched = all
                        .into_iter()
                        .filter(|row| {
                            row.series_fingerprint == wanted && row.timestamp_ns <= window_end
                        })
                        .collect::<Vec<_>>();
                    black_box(matched)
                });
            },
        );

        // What it does now: one `ListingTable` over the same object, and the
        // projection and predicate reach the Parquet reader.
        group.bench_with_input(
            BenchmarkId::new("parquet_scan", rows),
            &rows,
            |bencher, _| {
                bencher.to_async(&runtime).iter(|| async {
                    let ctx = SessionContext::new();
                    register_log_blocks_from_object_store(
                        &ctx,
                        "logs",
                        Arc::clone(&store),
                        &prefix,
                        std::slice::from_ref(&planned),
                    )
                    .expect("the planned block registers");
                    let batches = ctx
                        .sql(&format!(
                            "select series_fingerprint, timestamp_ns, line, structured_metadata \
                             from logs \
                             where series_fingerprint = {wanted} \
                             and timestamp_ns >= 0 and timestamp_ns <= {window_end} \
                             order by series_fingerprint, timestamp_ns"
                        ))
                        .await
                        .expect("the scan query plans")
                        .collect()
                        .await
                        .expect("the scan query runs");
                    black_box(batches)
                });
            },
        );
    }
    group.finish();
}

/// The block's series, as the `shard` label value that distinguishes them.
///
/// Eight of them, which is the shape a Loki stream selector resolves to after
/// the label index has pruned the block set: the query below asks for one.
const SHARDS: [&str; 8] = ["a", "b", "c", "d", "e", "f", "g", "h"];

/// `rows` rows spread over `fingerprints`, with structured metadata wide
/// enough that it is most of the block's bytes.
///
/// The metadata values come from the shared seeded generator, so two runs read
/// the same block, and they are distinct, so Parquet cannot dictionary-encode
/// the column away. A block whose payload compresses to nothing would make
/// both lines below measure the same handful of pages.
fn block_rows(rows: i64, fingerprints: &[u64]) -> Vec<LogRow> {
    let mut noise = Seeded::new(0x106_B10C);
    (0..rows)
        .map(|row| {
            let which = usize::try_from(row).expect("a row index fits a usize");
            let fingerprint = fingerprints[which % fingerprints.len()];
            let payload = format!(
                "{:016x}{:016x}{:016x}{:016x}",
                noise.next_u64(),
                noise.next_u64(),
                noise.next_u64(),
                noise.next_u64()
            );
            LogRow::new(
                fingerprint,
                row,
                format!("request {row} completed"),
                [("payload".to_string(), payload)].into_iter().collect(),
            )
        })
        .collect()
}

criterion_group!(benches, log_object_store_scan);
criterion_main!(benches);
