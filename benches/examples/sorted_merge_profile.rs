//! CPU and allocation driver for sorted merges.
//! Usage: `sorted_merge_profile` [iterations=5] [pattern=all] [runs=0]
//! A run count of zero selects 2, 8, 24 and 64 runs.

use std::{hint::black_box, sync::Arc, time::Instant};

use arrow::{
    array::UInt64Array,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use assert2::assert;
use futures::{StreamExt, stream};
use krabka_blockstore::{MERGE_BATCH_ROWS, SortedMerge};

type Row = (u64, u64, u64);

struct Fixture {
    schema: SchemaRef,
    runs: Vec<Vec<RecordBatch>>,
    expected: Vec<Row>,
}

impl Fixture {
    fn new(pattern: &str, run_count: u64) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::UInt64, false),
            Field::new("run", DataType::UInt64, false),
            Field::new("position", DataType::UInt64, false),
        ]));
        let per_run = 65_536_u64.div_ceil(run_count);
        let mut expected = Vec::new();
        let runs = (0..run_count)
            .map(|index| {
                let rows = (0..per_run)
                    .map(|position| {
                        let key = match pattern {
                            "interleaved" => (position * run_count + index) / 3,
                            "disjoint" => index * per_run + position,
                            _ => panic!("unknown pattern"),
                        };
                        (key, index, position)
                    })
                    .collect::<Vec<_>>();
                expected.extend_from_slice(&rows);
                rows.chunks(1_024)
                    .map(|chunk| {
                        RecordBatch::try_new(
                            Arc::clone(&schema),
                            vec![
                                Arc::new(UInt64Array::from_iter_values(chunk.iter().map(|r| r.0))),
                                Arc::new(UInt64Array::from_iter_values(chunk.iter().map(|r| r.1))),
                                Arc::new(UInt64Array::from_iter_values(chunk.iter().map(|r| r.2))),
                            ],
                        )
                        .expect("the columns match")
                    })
                    .collect()
            })
            .collect();
        expected.sort_by_key(|row| row.0);
        Self {
            schema,
            runs,
            expected,
        }
    }

    fn merge(&self) -> SortedMerge {
        let runs = self
            .runs
            .iter()
            .map(|batches| stream::iter(batches.clone().into_iter().map(Ok)).boxed())
            .collect();
        SortedMerge::new(
            Arc::clone(&self.schema),
            &["key".into()],
            runs,
            MERGE_BATCH_ROWS,
        )
        .expect("the sort key exists")
    }

    async fn verify(&self) {
        let mut merge = self.merge();
        let mut actual = Vec::new();
        while let Some(batch) = merge.next_batch().await.expect("the merge succeeds") {
            let columns = batch
                .columns()
                .iter()
                .map(|column| {
                    column
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .expect("a u64 column")
                })
                .collect::<Vec<_>>();
            actual.extend((0..batch.num_rows()).map(|row| {
                (
                    columns[0].value(row),
                    columns[1].value(row),
                    columns[2].value(row),
                )
            }));
        }
        assert!(actual == self.expected);
    }

    async fn drain(&self) {
        let mut merge = self.merge();
        let mut rows = 0;
        while let Some(batch) = merge.next_batch().await.expect("the merge succeeds") {
            rows += black_box(batch).num_rows();
        }
        assert!(rows == self.expected.len());
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let iterations = args
        .next()
        .map_or(5, |v| v.parse::<u32>().expect("iteration count"));
    let pattern = args.next().unwrap_or_else(|| "all".into());
    let run_count = args
        .next()
        .map_or(0, |v| v.parse::<u64>().expect("run count"));
    assert!(iterations > 0);
    assert!(["all", "interleaved", "disjoint"].contains(&pattern.as_str()));
    assert!([0, 2, 8, 24, 64].contains(&run_count));
    // Allocation counts are thread-local. Keep all merge work on this thread.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    println!("pattern,runs,rows,mean_us,allocations,allocated_bytes,peak_bytes");
    for shape in ["interleaved", "disjoint"] {
        if pattern != "all" && pattern != shape {
            continue;
        }
        for count in [2, 8, 24, 64] {
            if run_count != 0 && run_count != count {
                continue;
            }
            let fixture = Fixture::new(shape, count);
            allocation_counter::opt_out(|| runtime.block_on(fixture.verify()));
            let started = Instant::now();
            allocation_counter::opt_out(|| {
                for _ in 0..iterations {
                    runtime.block_on(fixture.drain());
                }
            });
            let mean_us = started.elapsed().as_secs_f64() * 1e6 / f64::from(iterations);
            let allocations = allocation_counter::measure(|| runtime.block_on(fixture.drain()));
            println!(
                "{shape},{count},{},{mean_us:.2},{},{},{}",
                fixture.expected.len(),
                allocations.count_total,
                allocations.bytes_total,
                allocations.bytes_max
            );
        }
    }
}
