//! Reading a Parquet block back from object storage.
//!
//! Two cases, and the gap between them is the measurement that matters. The
//! whole-block read decodes every row group; the row-group read decodes one.
//! If those two ever converge, row-group pruning has stopped pruning -- which
//! is the failure this design is most exposed to and the one least likely to
//! break a correctness test, because a query that scans everything still
//! returns the right answer.
//!
//! The two lines coincide today, at every size below. That is not a fault in
//! the measurement: `BlockWriter` builds its Parquet writer from a default
//! `WriterProperties`, whose maximum row group is 1,048,576 rows, so every
//! block this file writes is a single row group and reading "one row group" is
//! reading all of it. Row-group pruning therefore does nothing until a block
//! passes a million rows. The number is worth having written down, and the two
//! lines are worth keeping side by side: the day the writer starts cutting row
//! groups, this is where the gap appears, and if it does not appear then, the
//! pruning is not reaching the reader.

use std::{hint::black_box, sync::Arc};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use krabka_blockstore::{BlockWriter, read_block, read_block_row_groups, read_row_group_metadata};
use krabka_o11y_benches::{
    blocks::{block_schema, series_batches},
    index::TENANT,
};
use object_store::{ObjectStore, memory::InMemory};
use tokio::runtime::Runtime;

const CASES: [(usize, usize); 3] = [(1_000, 10), (10_000, 10), (100_000, 10)];

const KEY: &str = "bench/block.parquet";

fn blockstore_read(criterion: &mut Criterion) {
    let runtime = Runtime::new().expect("a tokio runtime");
    let mut group = criterion.benchmark_group("blockstore_read");
    group.sample_size(10);

    for (series, samples) in CASES {
        let rows = series * samples;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let row_groups = runtime.block_on(async {
            BlockWriter::new(Arc::clone(&store))
                .write_block(
                    TENANT,
                    KEY,
                    block_schema(),
                    &series_batches(series, samples),
                )
                .await
                .expect("the fixture block is valid");
            read_row_group_metadata(Arc::clone(&store), KEY)
                .await
                .expect("a block just written has metadata")
        });
        assert!(
            !row_groups.is_empty(),
            "a block of {rows} rows has at least one row group"
        );

        group.throughput(Throughput::Elements(
            u64::try_from(rows).expect("a row count fits a u64"),
        ));
        group.bench_with_input(
            BenchmarkId::new("whole_block", rows),
            &rows,
            |bencher, _| {
                bencher.to_async(&runtime).iter(|| async {
                    let batches = read_block(Arc::clone(&store), KEY)
                        .await
                        .expect("the block reads back");
                    black_box(batches)
                });
            },
        );

        // One row group out of however many the block holds. Its cost should
        // track the row group's size rather than the block's, so this line
        // should stay near flat across the sweep while `whole_block` rises.
        group.bench_with_input(
            BenchmarkId::new("one_row_group", rows),
            &rows,
            |bencher, _| {
                bencher.to_async(&runtime).iter(|| async {
                    let batches = read_block_row_groups(Arc::clone(&store), KEY, &[0])
                        .await
                        .expect("row group zero exists");
                    black_box(batches)
                });
            },
        );

        // Metadata alone: what a planner pays before it has decided which row
        // groups it wants. A query that prunes well spends most of its read
        // budget here, so this is the floor the two above are measured against.
        group.bench_with_input(BenchmarkId::new("metadata", rows), &rows, |bencher, _| {
            bencher.to_async(&runtime).iter(|| async {
                let meta = read_row_group_metadata(Arc::clone(&store), KEY)
                    .await
                    .expect("the block has metadata");
                black_box(meta)
            });
        });
    }
    group.finish();
}

criterion_group!(benches, blockstore_read);
criterion_main!(benches);
