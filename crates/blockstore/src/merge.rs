//! Merging sorted blocks without holding them.
//!
//! A compaction reads several blocks and writes one. The blocks it reads are
//! each already in their declaration's sort order, so the merged block can be
//! produced in that order by a k-way merge that never holds more than one
//! batch of each input -- and a merge whose output is already sorted is also
//! the input [`BlockWriter::open_block`](crate::BlockWriter::open_block)
//! wants, which is what lets the whole compaction cost its output rather than
//! the sum of its inputs.

use std::{
    hash::{DefaultHasher, Hash, Hasher},
    ops::Range,
    sync::Arc,
};

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

    #[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
    struct MergeRow {
        fingerprint: u64,
        timestamp: i64,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct SourceRow {
        fingerprint: u64,
        source: &'static str,
    }

    struct MergeDrain {
        rows: Vec<MergeRow>,
        batch_sizes: Vec<usize>,
    }

    fn series_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(crate::COL_FINGERPRINT, DataType::UInt64, false),
            Field::new(crate::COL_TIMESTAMP, DataType::Int64, false),
        ]))
    }

    /// A batch of `(fingerprint, timestamp)` pairs, in the order given.
    fn batch(rows: &[MergeRow]) -> RecordBatch {
        let fingerprints = UInt64Array::from_iter_values(rows.iter().map(|row| row.fingerprint));
        let timestamps = Int64Array::from_iter_values(rows.iter().map(|row| row.timestamp));
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
    fn pairs(batch: &RecordBatch) -> Vec<MergeRow> {
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
            .map(|row| MergeRow {
                fingerprint: fingerprints.value(row),
                timestamp: timestamps.value(row),
            })
            .collect()
    }

    /// Every row every run holds, in the order the merge emits them, and the
    /// size of each batch it emitted them in.
    async fn drain(merge: &mut SortedMerge) -> MergeDrain {
        let mut rows = Vec::new();
        let mut sizes = Vec::new();
        while let Some(batch) = merge.next_batch().await.expect("the runs merge") {
            sizes.push(batch.num_rows());
            rows.extend(pairs(&batch));
        }
        MergeDrain {
            rows,
            batch_sizes: sizes,
        }
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
                run(vec![
                    batch(&[
                        MergeRow {
                            fingerprint: 1,
                            timestamp: 10,
                        },
                        MergeRow {
                            fingerprint: 4,
                            timestamp: 40,
                        },
                    ]),
                    batch(&[MergeRow {
                        fingerprint: 7,
                        timestamp: 70,
                    }]),
                ]),
                run(vec![batch(&[
                    MergeRow {
                        fingerprint: 2,
                        timestamp: 20,
                    },
                    MergeRow {
                        fingerprint: 5,
                        timestamp: 50,
                    },
                    MergeRow {
                        fingerprint: 8,
                        timestamp: 80,
                    },
                ])]),
                run(vec![
                    batch(&[MergeRow {
                        fingerprint: 3,
                        timestamp: 30,
                    }]),
                    batch(&[
                        MergeRow {
                            fingerprint: 6,
                            timestamp: 60,
                        },
                        MergeRow {
                            fingerprint: 9,
                            timestamp: 90,
                        },
                    ]),
                ]),
            ],
            MERGE_BATCH_ROWS,
        )
        .expect("the runs share the sort key");

        let drained = drain(&mut merge).await;
        let rows = drained.rows;

        assert!(
            rows == (1..=9_u64)
                .map(|value| MergeRow {
                    fingerprint: value,
                    timestamp: i64::try_from(value).expect("a small value fits") * 10,
                })
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
                run(vec![batch(&[
                    MergeRow {
                        fingerprint: 1,
                        timestamp: 5,
                    },
                    MergeRow {
                        fingerprint: 1,
                        timestamp: 5,
                    },
                ])]),
                run(vec![batch(&[MergeRow {
                    fingerprint: 1,
                    timestamp: 5,
                }])]),
            ],
            MERGE_BATCH_ROWS,
        )
        .expect("the runs share the sort key");

        let drained = drain(&mut merge).await;
        let rows = drained.rows;

        assert!(
            rows == vec![
                MergeRow {
                    fingerprint: 1,
                    timestamp: 5
                },
                MergeRow {
                    fingerprint: 1,
                    timestamp: 5
                },
                MergeRow {
                    fingerprint: 1,
                    timestamp: 5
                }
            ]
        );
    }

    #[tokio::test]
    async fn a_later_run_cannot_carry_an_equal_key_past_an_earlier_run() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(crate::COL_FINGERPRINT, DataType::UInt64, false),
            Field::new(crate::COL_TIMESTAMP, DataType::Int64, false),
            Field::new("source", DataType::Utf8, false),
        ]));
        let batch = |rows: &[SourceRow]| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(UInt64Array::from_iter_values(
                        rows.iter().map(|row| row.fingerprint),
                    )),
                    Arc::new(Int64Array::from_iter_values(rows.iter().map(|_| 1))),
                    Arc::new(StringArray::from_iter_values(
                        rows.iter().map(|row| row.source),
                    )),
                ],
            )
            .expect("the columns match the schema")
        };
        let mut merge = SortedMerge::new(
            schema.clone(),
            &sort_key(),
            vec![
                run(vec![batch(&[SourceRow {
                    fingerprint: 2,
                    source: "earlier",
                }])]),
                run(vec![batch(&[
                    SourceRow {
                        fingerprint: 1,
                        source: "first",
                    },
                    SourceRow {
                        fingerprint: 2,
                        source: "later",
                    },
                ])]),
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
            .map(|row| MergeRow {
                fingerprint: row * 2,
                timestamp: 1_i64,
            })
            .collect::<Vec<_>>();
        let other = (0..1_000_u64)
            .map(|row| MergeRow {
                fingerprint: row * 4 + 1,
                timestamp: 1_i64,
            })
            .collect::<Vec<_>>();
        let mut merge = SortedMerge::new(
            series_schema(),
            &sort_key(),
            vec![run(vec![batch(&one)]), run(vec![batch(&other)])],
            1_000,
        )
        .expect("the runs share the sort key");

        let drained = drain(&mut merge).await;
        let rows = drained.rows;
        let sizes = drained.batch_sizes;

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
                run(vec![batch(&[
                    MergeRow {
                        fingerprint: 1,
                        timestamp: 1,
                    },
                    MergeRow {
                        fingerprint: 2,
                        timestamp: 2,
                    },
                ])]),
            ],
            MERGE_BATCH_ROWS,
        )
        .expect("the runs share the sort key");

        let drained = drain(&mut merge).await;
        let rows = drained.rows;
        let sizes = drained.batch_sizes;

        check!(
            rows == vec![
                MergeRow {
                    fingerprint: 1,
                    timestamp: 1
                },
                MergeRow {
                    fingerprint: 2,
                    timestamp: 2
                }
            ]
        );
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
            .map(|row| MergeRow {
                fingerprint: row,
                timestamp: i64::try_from(row).expect("a row index fits an i64"),
            })
            .collect::<Vec<_>>();
        BlockWriter::new(Arc::clone(&store))
            .write_block("t", "b.parquet", series_schema(), &[batch(&rows)])
            .await
            .expect("the block is written");

        let opened = open_block_stream(
            Arc::clone(&store),
            "b.parquet",
            DEFAULT_BLOCK_READ_MAX,
            1_000,
        )
        .await
        .expect("the block opens");
        let schema = opened.schema;
        let streamed = opened
            .batches
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
            .write_block(
                "t",
                "b.parquet",
                series_schema(),
                &[batch(&[MergeRow {
                    fingerprint: 1,
                    timestamp: 1,
                }])],
            )
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
            (0..600_u64)
                .map(|row| MergeRow {
                    fingerprint: row * 3,
                    timestamp: 1_i64,
                })
                .collect::<Vec<_>>(),
            (0..600_u64)
                .map(|row| MergeRow {
                    fingerprint: row * 3 + 1,
                    timestamp: 2_i64,
                })
                .collect::<Vec<_>>(),
            (0..600_u64)
                .map(|row| MergeRow {
                    fingerprint: row * 3 + 2,
                    timestamp: 3_i64,
                })
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
            let opened = open_block_stream(
                Arc::clone(&store),
                &format!("in-{index}.parquet"),
                DEFAULT_BLOCK_READ_MAX,
                256,
            )
            .await
            .expect("the block opens");
            runs.push(opened.batches);
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

mod block_object_reader;
mod run_cursor;
mod sorted_merge;

use block_object_reader::BlockObjectReader;
mod versioned_compaction_key;
use run_cursor::RunCursor;
pub use sorted_merge::SortedMerge;
pub use versioned_compaction_key::versioned_compaction_key;

/// One compaction input as the batches it yields.
pub type BlockBatchStream = BoxStream<'static, Result<RecordBatch>>;

/// Rows per batch emitted by a sorted merge.
pub const MERGE_BATCH_ROWS: usize = 8_192;

/// Rows decoded from each merge input batch.
pub const MERGE_READ_BATCH_ROWS: usize = 1_024;

/// Wraps an object-store error so the Parquet reader can carry it, and a
/// caller can get it back out.
///
/// `ParquetError::External` keeps the error whole rather than stringifying it,
/// which is what lets
/// [`BlockReadFailure`](crate::BlockReadFailure) tell a missing block from a
/// store that is down after the failure has been through the Parquet reader.
pub(crate) fn to_parquet_error(error: object_store::Error) -> ParquetError {
    ParquetError::External(Box::new(error))
}

/// The metadata, schema, and row stream opened from one block.
pub struct OpenBlockStream {
    pub meta: ObjectMeta,
    pub schema: SchemaRef,
    pub batches: BlockBatchStream,
}

/// Opens the block at `object_key` as a stream of batches, and reports the
/// schema it was written with.
///
/// The schema comes back before any row does, which is what lets a caller
/// settle the merged output's schema -- the union of its inputs' promoted
/// columns, say -- from the footers alone rather than by reading the blocks.
///
/// Unlike [`read_block_with_max_bytes`](crate::read_block_with_max_bytes) this
/// leaves the block on the object store: the returned stream fetches and
/// decodes a row group at a time, so an input costs one batch of memory rather
/// than all of it. `max_bytes` is still checked against the object's on-disk
/// size, for the same reason the buffered reader checks it -- an
/// unexpectedly huge block is a sign something is wrong upstream, and it is
/// better to say so than to grind through it.
///
/// # Errors
/// Returns [`BlockStoreError::BlockUnreadable`] when the object is missing or
/// is not a Parquet block, [`BlockStoreError::InvalidBlock`] when it is larger
/// than `max_bytes`, and [`BlockStoreError::ObjectStore`] when the store
/// itself fails.
pub async fn open_block_stream(
    store: Arc<dyn ObjectStore>,
    object_key: &str,
    max_bytes: ByteSize,
    batch_rows: usize,
) -> Result<OpenBlockStream> {
    let path = Path::from(object_key);
    let meta = store.head(&path).await.map_err(|error| {
        BlockStoreError::block_unreadable(object_key, BlockReadFailure::ObjectStore(error))
    })?;
    if ByteSize::from_bytes(meta.size) > max_bytes {
        return Err(BlockStoreError::InvalidBlock(format!(
            "block `{object_key}` is {} bytes, exceeds cap of {} bytes",
            meta.size,
            max_bytes.bytes_u64()
        )));
    }

    let reader = BlockObjectReader::new(store, meta.clone());
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|error| {
            BlockStoreError::block_unreadable(object_key, BlockReadFailure::Parquet(error))
        })?;
    crate::validate_persisted_block_format(builder.metadata())
        .map_err(BlockStoreError::InvalidBlock)?;
    let builder = builder.with_batch_size(batch_rows);
    let key = object_key.to_string();
    let batches = builder.build().map_err(|error| {
        BlockStoreError::block_unreadable(object_key, BlockReadFailure::Parquet(error))
    })?;
    // The built stream preserves Arrow types such as dictionaries while
    // stripping file-level Parquet metadata from the logical batch schema.
    let schema = Arc::clone(batches.schema());
    let batches = batches
        .map_err(move |error| {
            BlockStoreError::block_unreadable(&key, BlockReadFailure::Parquet(error))
        })
        .boxed();
    Ok(OpenBlockStream {
        meta,
        schema,
        batches,
    })
}
