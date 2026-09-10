//! Writing a Parquet block to object storage.
//!
//! The block store's whole thesis is that a columnar block on object storage
//! is the right unit, so what one block costs to write is the first number
//! worth having. The sweep is over rows per block: encoding is expected to be
//! linear in them, and a curve that bends says the writer gained a per-row cost
//! that does not belong there -- a re-encode, a per-row allocation, a
//! statistics pass that grew a factor.

use std::{hint::black_box, sync::Arc};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use krabka_blockstore::BlockWriter;
use krabka_o11y_benches::{
    blocks::{block_schema, series_batches},
    index::TENANT,
};
use object_store::{ObjectStore, memory::InMemory};
use tokio::runtime::Runtime;

/// Rows per block, and the samples per series that produce them.
///
/// Ten samples a series is a two-and-a-half minute block at a fifteen-second
/// scrape, which is small; the number under test is the row count, and holding
/// samples fixed while series rises is what keeps the fingerprint column's
/// cardinality rising with it. A block whose every row is the same series
/// compresses to nothing and would measure the dictionary encoder instead.
const CASES: [(usize, usize); 3] = [(1_000, 10), (10_000, 10), (100_000, 10)];

fn blockstore_write(criterion: &mut Criterion) {
    let runtime = Runtime::new().expect("a tokio runtime");
    let mut group = criterion.benchmark_group("blockstore_write");
    // A megabyte-scale Parquet write is milliseconds, not nanoseconds, so
    // Criterion's default of 100 samples is minutes of wall clock for one
    // point. Ten is enough to see an order-of-magnitude change, which is what
    // the ratchet gates on.
    group.sample_size(10);

    for (series, samples) in CASES {
        let rows = series * samples;
        let batches = series_batches(series, samples);
        let schema = block_schema();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = BlockWriter::new(Arc::clone(&store));

        group.throughput(Throughput::Elements(
            u64::try_from(rows).expect("a row count fits a u64"),
        ));
        group.bench_with_input(BenchmarkId::new("parquet", rows), &rows, |bencher, _| {
            bencher.to_async(&runtime).iter(|| async {
                // The same key every iteration, which the store overwrites, so
                // a long run does not measure a store that grew to gigabytes.
                let meta = writer
                    .write_block(TENANT, "bench/block.parquet", Arc::clone(&schema), &batches)
                    .await
                    .expect("the fixture block is valid");
                black_box(meta)
            });
        });
    }
    group.finish();
}

criterion_group!(benches, blockstore_write);
criterion_main!(benches);
