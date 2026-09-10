//! Merging sorted blocks without holding them.
//!
//! A compaction reads several blocks and writes one. The blocks it reads are
//! each already in their declaration's sort order, so the merged block can be
//! produced in that order by a k-way merge that never holds more than one
//! batch of each input -- and a merge whose output is already sorted is also
//! the input [`BlockWriter::open_block`](crate::BlockWriter::open_block)
//! wants, which is what lets the whole compaction cost its output rather than
//! the sum of its inputs.

use std::{ops::Range, sync::Arc};

use arrow::{
    array::ArrayRef,
    compute::concat_batches,
    datatypes::SchemaRef,
    record_batch::RecordBatch,
    row::{Row, RowConverter, Rows, SortField},
};
use bytes::Bytes;
use futures::{
    FutureExt, StreamExt, TryStreamExt,
    future::{BoxFuture, try_join_all},
    stream::BoxStream,
};
use krabka_units::prelude::*;
use object_store::{GetOptions, ObjectMeta, ObjectStore, ObjectStoreExt, path::Path};
use parquet::{
    arrow::{
        ParquetRecordBatchStreamBuilder, arrow_reader::ArrowReaderOptions,
        async_reader::AsyncFileReader,
    },
    errors::ParquetError,
    file::metadata::{ParquetMetaData, ParquetMetaDataReader},
};

use crate::{
    error::{BlockReadFailure, BlockStoreError, Result},
    writer::SORT_KEY_OPTIONS,
};

#[cfg(test)]
mod tests {
    use arrow::{
        array::{Int64Array, StringArray, UInt64Array},
        datatypes::{DataType, Field, Schema},
    };
    use assert2::{assert, check};
    use futures::stream;
    use object_store::memory::InMemory;

    use super::*;
    use crate::{
        BlockWriter, SummaryColumns, block_index::series_block_schema, read_block,
        reader::DEFAULT_BLOCK_READ_MAX,
    };

    fn series_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(crate::COL_FINGERPRINT, DataType::UInt64, false),
            Field::new(crate::COL_TIMESTAMP, DataType::Int64, false),
        ]))
    }

    /// A batch of `(fingerprint, timestamp)` pairs, in the order given.
    fn batch(rows: &[(u64, i64)]) -> RecordBatch {
        let fingerprints = UInt64Array::from_iter_values(rows.iter().map(|(fp, _)| *fp));
        let timestamps = Int64Array::from_iter_values(rows.iter().map(|(_, ts)| *ts));
        RecordBatch::try_new(
            series_schema(),
            vec![Arc::new(fingerprints), Arc::new(timestamps)],
        )
        .expect("the columns match the schema")
    }

    /// A run that yields `batches` and never fails.
    fn run(batches: Vec<RecordBatch>) -> BlockBatchStream {
        stream::iter(batches.into_iter().map(Ok)).boxed()
    }

    /// The `(fingerprint, timestamp)` pairs of `batch`, in row order.
    fn pairs(batch: &RecordBatch) -> Vec<(u64, i64)> {
        let fingerprints = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("a u64 column");
        let timestamps = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("an i64 column");
        (0..batch.num_rows())
            .map(|row| (fingerprints.value(row), timestamps.value(row)))
            .collect()
    }

    /// Every row every run holds, in the order the merge emits them, and the
    /// size of each batch it emitted them in.
    async fn drain(merge: &mut SortedMerge) -> (Vec<(u64, i64)>, Vec<usize>) {
        let mut rows = Vec::new();
        let mut sizes = Vec::new();
        while let Some(batch) = merge.next_batch().await.expect("the runs merge") {
            sizes.push(batch.num_rows());
            rows.extend(pairs(&batch));
        }
        (rows, sizes)
    }

    fn sort_key() -> Vec<String> {
        series_block_schema().sort_key
    }

    #[tokio::test]
    async fn a_merge_of_sorted_runs_is_the_rows_of_all_of_them_in_order() {
        let mut merge = SortedMerge::new(
            series_schema(),
            &sort_key(),
            vec![
                run(vec![batch(&[(1, 10), (4, 40)]), batch(&[(7, 70)])]),
                run(vec![batch(&[(2, 20), (5, 50), (8, 80)])]),
                run(vec![batch(&[(3, 30)]), batch(&[(6, 60), (9, 90)])]),
            ],
            MERGE_BATCH_ROWS,
        )
        .expect("the runs share the sort key");

        let (rows, _) = drain(&mut merge).await;

        assert!(
            rows == (1..=9_u64)
                .map(|value| (
                    value,
                    i64::try_from(value).expect("a small value fits") * 10
                ))
                .collect::<Vec<_>>()
        );
    }

    /// Rows the declared key does not separate keep the order the runs were
    /// listed in, so a merge of the same inputs twice is the same block twice.
    #[tokio::test]
    async fn rows_equal_on_the_key_come_out_in_run_order() {
        let mut merge = SortedMerge::new(
            series_schema(),
            &sort_key(),
            vec![
                run(vec![batch(&[(1, 5), (1, 5)])]),
                run(vec![batch(&[(1, 5)])]),
            ],
            MERGE_BATCH_ROWS,
        )
        .expect("the runs share the sort key");

        let (rows, _) = drain(&mut merge).await;

        assert!(rows == vec![(1, 5), (1, 5), (1, 5)]);
    }

    #[tokio::test]
    async fn a_later_run_cannot_carry_an_equal_key_past_an_earlier_run() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(crate::COL_FINGERPRINT, DataType::UInt64, false),
            Field::new(crate::COL_TIMESTAMP, DataType::Int64, false),
            Field::new("source", DataType::Utf8, false),
        ]));
        let batch = |rows: &[(u64, &'static str)]| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(UInt64Array::from_iter_values(rows.iter().map(|row| row.0))),
                    Arc::new(Int64Array::from_iter_values(rows.iter().map(|_| 1))),
                    Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
                ],
            )
            .expect("the columns match the schema")
        };
        let mut merge = SortedMerge::new(
            schema.clone(),
            &sort_key(),
            vec![
                run(vec![batch(&[(2, "earlier")])]),
                run(vec![batch(&[(1, "first"), (2, "later")])]),
            ],
            MERGE_BATCH_ROWS,
        )
        .expect("the runs share the sort key");
        let mut sources = Vec::new();
        while let Some(batch) = merge.next_batch().await.expect("the runs merge") {
            let source = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("a string column");
            sources.extend(source.iter().flatten().map(str::to_string));
        }

        assert!(sources == vec!["first", "earlier", "later"]);
    }

    /// The batch size is the merge's whole resident cost, so it has to be a
    /// bound and not a suggestion: three thousand rows through a merge that
    /// emits a thousand at a time is three batches, whatever the inputs looked
    /// like.
    #[tokio::test]
    async fn the_emitted_batches_are_capped_however_large_the_inputs_are() {
        let one = (0..2_000_u64)
            .map(|row| (row * 2, 1_i64))
            .collect::<Vec<_>>();
        let other = (0..1_000_u64)
            .map(|row| (row * 4 + 1, 1_i64))
            .collect::<Vec<_>>();
        let mut merge = SortedMerge::new(
            series_schema(),
            &sort_key(),
            vec![run(vec![batch(&one)]), run(vec![batch(&other)])],
            1_000,
        )
        .expect("the runs share the sort key");

        let (rows, sizes) = drain(&mut merge).await;

        check!(rows.len() == 3_000);
        check!(sizes == vec![1_000; 3]);
        check!(
            rows.windows(2).all(|pair| pair[0] <= pair[1]),
            "the merged rows are in the declared order"
        );
    }

    /// An input that yields empty batches, or nothing at all, is not a row of
    /// anything -- a merge whose planner over-counted an input must still
    /// produce the other inputs' rows rather than stall on the empty one.
    #[tokio::test]
    async fn empty_and_exhausted_runs_contribute_nothing() {
        let mut merge = SortedMerge::new(
            series_schema(),
            &sort_key(),
            vec![
                run(vec![RecordBatch::new_empty(series_schema())]),
                run(Vec::new()),
                run(vec![batch(&[(1, 1), (2, 2)])]),
            ],
            MERGE_BATCH_ROWS,
        )
        .expect("the runs share the sort key");

        let (rows, sizes) = drain(&mut merge).await;

        check!(rows == vec![(1, 1), (2, 2)]);
        check!(sizes == vec![2]);
    }

    #[tokio::test]
    async fn a_merge_without_a_sort_key_is_refused() {
        let merge = SortedMerge::new(series_schema(), &[], Vec::new(), MERGE_BATCH_ROWS);

        assert!(let Err(BlockStoreError::InvalidBlock(_)) = merge);
    }

    /// The stream reads back what the writer wrote, and reports the schema
    /// before it reads a single row -- which is what a compactor settles its
    /// output schema from.
    #[tokio::test]
    async fn a_block_stream_reads_back_the_block_the_writer_wrote() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rows = (0..2_500_u64)
            .map(|row| (row, i64::try_from(row).expect("a row index fits an i64")))
            .collect::<Vec<_>>();
        BlockWriter::new(Arc::clone(&store))
            .write_block("t", "b.parquet", series_schema(), &[batch(&rows)])
            .await
            .expect("the block is written");

        let (schema, batches) = open_block_stream(
            Arc::clone(&store),
            "b.parquet",
            DEFAULT_BLOCK_READ_MAX,
            1_000,
        )
        .await
        .expect("the block opens");
        let streamed = batches
            .try_collect::<Vec<_>>()
            .await
            .expect("the block reads");

        check!(schema == series_schema());
        check!(
            streamed
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>()
                == vec![1_000, 1_000, 500],
            "the stream decodes a batch at a time rather than the whole block"
        );
        let buffered = read_block(store, "b.parquet")
            .await
            .expect("the block reads");
        check!(
            concat_batches(&schema, &streamed).expect("the batches concatenate")
                == concat_batches(&schema, &buffered).expect("the batches concatenate")
        );
    }

    /// The cap is checked against the object's own size before a byte of it is
    /// fetched, exactly as the buffered reader checks it.
    #[tokio::test]
    async fn a_block_over_the_cap_is_refused_before_it_is_read() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        BlockWriter::new(Arc::clone(&store))
            .write_block("t", "b.parquet", series_schema(), &[batch(&[(1, 1)])])
            .await
            .expect("the block is written");

        let opened = open_block_stream(store, "b.parquet", krabka_units::bytes(1), 1_000).await;

        assert!(
            let Err(BlockStoreError::InvalidBlock(message)) = &opened,
        );
        check!(message.contains("exceeds cap"));
    }

    #[tokio::test]
    async fn a_missing_block_is_that_blocks_failure_rather_than_the_stores() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let opened = open_block_stream(store, "gone.parquet", DEFAULT_BLOCK_READ_MAX, 1_000).await;

        assert!(let Err(error) = &opened);
        check!(error.is_block_missing());
    }

    /// A merge is only worth writing if the block it produces is the block a
    /// buffered compaction would have produced. Same rows, same order, same
    /// metadata -- the difference is meant to be in the memory alone.
    #[tokio::test]
    async fn a_block_written_from_a_merge_matches_one_written_from_the_whole_input() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = BlockWriter::new(Arc::clone(&store));
        let inputs = [
            (0..600_u64).map(|row| (row * 3, 1_i64)).collect::<Vec<_>>(),
            (0..600_u64)
                .map(|row| (row * 3 + 1, 2_i64))
                .collect::<Vec<_>>(),
            (0..600_u64)
                .map(|row| (row * 3 + 2, 3_i64))
                .collect::<Vec<_>>(),
        ];
        for (index, rows) in inputs.iter().enumerate() {
            writer
                .write_block(
                    "t",
                    &format!("in-{index}.parquet"),
                    series_schema(),
                    &[batch(rows)],
                )
                .await
                .expect("the block is written");
        }

        let mut runs = Vec::new();
        for index in 0..inputs.len() {
            let (_, batches) = open_block_stream(
                Arc::clone(&store),
                &format!("in-{index}.parquet"),
                DEFAULT_BLOCK_READ_MAX,
                256,
            )
            .await
            .expect("the block opens");
            runs.push(batches);
        }
        let mut merge = SortedMerge::new(series_schema(), &sort_key(), runs, 256)
            .expect("the runs share the sort key");
        let mut block = writer
            .open_block(
                "t",
                "merged.parquet",
                series_schema(),
                &series_block_schema(),
                SummaryColumns::series(),
            )
            .expect("the block opens");
        while let Some(merged) = merge.next_batch().await.expect("the runs merge") {
            block.write_batch(&merged).await.expect("the batch writes");
        }
        let merged = block.finish().await.expect("the block closes");

        let whole = inputs.concat();
        let buffered = writer
            .write_block("t", "buffered.parquet", series_schema(), &[batch(&whole)])
            .await
            .expect("the block is written");
        check!(
            merged
                == crate::BlockMeta {
                    object_key: "merged.parquet".to_string(),
                    ..buffered
                }
        );

        let read = |key: &'static str| {
            let store = Arc::clone(&store);
            async move {
                let batches = read_block(store, key).await.expect("the block reads");
                concat_batches(&series_schema(), &batches).expect("the batches concatenate")
            }
        };
        check!(read("merged.parquet").await == read("buffered.parquet").await);
    }
}

mod block_batch_stream;
mod block_object_reader;
mod merge_batch_rows;
mod merge_read_batch_rows;
mod open_block_stream;
mod run_cursor;
mod sorted_merge;
mod to_parquet_error;

pub use block_batch_stream::BlockBatchStream;
use block_object_reader::BlockObjectReader;
pub use merge_batch_rows::MERGE_BATCH_ROWS;
pub use merge_read_batch_rows::MERGE_READ_BATCH_ROWS;
pub use open_block_stream::open_block_stream;
use run_cursor::RunCursor;
pub use sorted_merge::SortedMerge;
use to_parquet_error::to_parquet_error;
