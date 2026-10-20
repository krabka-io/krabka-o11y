use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use arrow::{
    array::UInt64Array,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use assert2::{assert, check};
use futures::{FutureExt, StreamExt, channel::mpsc, stream};
use krabka_blockstore::{BlockBatchStream, BlockStoreError, SortedMerge};

type Row = (u64, u64, u64);

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("run", DataType::UInt64, false),
        Field::new("position", DataType::UInt64, false),
    ]))
}

fn batch(rows: &[Row]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(UInt64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(UInt64Array::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(UInt64Array::from_iter_values(rows.iter().map(|row| row.2))),
        ],
    )
    .expect("the columns match")
}

fn rows(batch: &RecordBatch) -> Vec<Row> {
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
    (0..batch.num_rows())
        .map(|index| {
            (
                columns[0].value(index),
                columns[1].value(index),
                columns[2].value(index),
            )
        })
        .collect()
}

fn run(batches: Vec<RecordBatch>) -> BlockBatchStream {
    stream::iter(batches.into_iter().map(Ok)).boxed()
}

#[tokio::test]
async fn complete_output_matches_a_stable_sort_across_run_and_batch_counts() {
    for run_count in [0, 1, 2, 3, 8, 24, 64] {
        for pattern in ["interleaved", "disjoint", "equal"] {
            for batch_rows in [0, 1, 7, 128] {
                let mut expected = Vec::new();
                let mut runs = Vec::new();
                for index in 0..run_count {
                    let input = (0..17 + index % 5)
                        .map(|position| {
                            let key = match pattern {
                                "interleaved" => position * run_count + index,
                                "disjoint" => index * 32 + position,
                                _ => position / 3,
                            };
                            (key, index, position)
                        })
                        .collect::<Vec<_>>();
                    expected.extend_from_slice(&input);
                    let mut batches = vec![batch(&[])];
                    for chunk in input.chunks(3) {
                        batches.push(batch(chunk));
                        batches.push(batch(&[]));
                    }
                    runs.push(run(batches));
                }
                expected.sort_by_key(|row| row.0);
                let mut merge = SortedMerge::new(schema(), &["key".into()], runs, batch_rows)
                    .expect("the sort key exists");
                let mut actual = Vec::new();
                while let Some(output) = merge.next_batch().await.expect("the merge succeeds") {
                    assert!(output.num_rows() > 0);
                    assert!(output.num_rows() <= batch_rows.max(1));
                    actual.extend(rows(&output));
                }
                assert!(
                    actual == expected,
                    "runs={run_count}, pattern={pattern}, limit={batch_rows}"
                );
                assert!(merge.next_batch().await.expect("exhausted").is_none());
            }
        }
    }
}

#[tokio::test]
async fn exhausted_streams_are_not_polled_again() {
    let polls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&polls);
    let exhausted = stream::poll_fn(move |_| {
        counted.fetch_add(1, Ordering::Relaxed);
        std::task::Poll::Ready(None)
    })
    .boxed();
    let mut merge = SortedMerge::new(
        schema(),
        &["key".into()],
        vec![
            exhausted,
            run(vec![batch(&[(1, 1, 0)]), batch(&[(2, 1, 1)])]),
        ],
        1,
    )
    .expect("the sort key exists");
    while merge
        .next_batch()
        .await
        .expect("the merge succeeds")
        .is_some()
    {}
    assert!(merge.next_batch().await.expect("exhausted").is_none());
    assert!(polls.load(Ordering::Relaxed) == 1);
}

#[tokio::test]
async fn read_errors_are_returned_during_initial_load_and_winner_refill() {
    for initial in [true, false] {
        let polls = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&polls);
        let exhausted = stream::poll_fn(move |_| {
            counted.fetch_add(1, Ordering::Relaxed);
            std::task::Poll::Ready(None)
        })
        .boxed();
        let mut batches = Vec::new();
        if !initial {
            batches.push(Ok(batch(&[(0, 0, 0)])));
        }
        batches.push(Err(BlockStoreError::InvalidBlock("read failed".into())));
        batches.push(Ok(batch(&[(2, 0, 1)])));
        let mut merge = SortedMerge::new(
            schema(),
            &["key".into()],
            vec![
                exhausted,
                stream::iter(batches).boxed(),
                run(vec![batch(&[(1, 1, 0), (3, 1, 1)])]),
            ],
            1,
        )
        .expect("the sort key exists");
        if !initial {
            let output = merge
                .next_batch()
                .await
                .expect("first batch")
                .expect("one row");
            assert!(rows(&output) == vec![(0, 0, 0)]);
        }
        assert!(
            let Err(BlockStoreError::InvalidBlock(message)) = merge.next_batch().await
        );
        assert!(message == "read failed");
        let mut actual = Vec::new();
        while let Some(output) = merge.next_batch().await.expect("retry succeeds") {
            actual.extend(rows(&output));
        }
        check!(actual == vec![(1, 1, 0), (2, 0, 1), (3, 1, 1)]);
        check!(polls.load(Ordering::Relaxed) == 1);
    }
}

#[tokio::test]
async fn a_cancelled_refill_keeps_the_consumed_run_pending() {
    let (sender, receiver) = mpsc::unbounded();
    let waiting = stream::iter(vec![Ok(batch(&[(0, 0, 0)]))])
        .chain(receiver)
        .boxed();
    let mut merge = SortedMerge::new(
        schema(),
        &["key".into()],
        vec![waiting, run(vec![batch(&[(1, 1, 0), (3, 1, 1)])])],
        1,
    )
    .expect("the sort key exists");
    let first = merge
        .next_batch()
        .await
        .expect("first batch")
        .expect("one row");
    assert!(rows(&first) == vec![(0, 0, 0)]);
    assert!(merge.next_batch().now_or_never().is_none());
    sender
        .unbounded_send(Ok(batch(&[])))
        .expect("the receiver exists");
    sender
        .unbounded_send(Ok(batch(&[(2, 0, 1)])))
        .expect("the receiver exists");
    drop(sender);
    let mut actual = Vec::new();
    while let Some(output) = merge.next_batch().await.expect("refill succeeds") {
        actual.extend(rows(&output));
    }
    assert!(actual == vec![(1, 1, 0), (2, 0, 1), (3, 1, 1)]);
}
