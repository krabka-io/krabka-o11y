//! Arrow batches shaped like a metrics block, for the write and read paths.

use std::sync::Arc;

use arrow::{
    array::{Float64Array, Int64Array, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use krabka_blockstore::{COL_FINGERPRINT, COL_TIMESTAMP, Labels};

use crate::{Seeded, index::series_labels};

/// Rows per `RecordBatch`, and so rows per Parquet row group.
///
/// A block written as one batch is one row group, and a reader that prunes by
/// row group then has nothing to prune. Splitting at a fixed size is what the
/// ingest path does and what makes the read benchmark measure a realistic
/// number of row groups rather than one.
pub const ROWS_PER_BATCH: usize = 8_192;

/// The Arrow schema a metrics block carries.
///
/// The two mandatory columns plus one payload column. `series_block_schema()`
/// constrains only the mandatory pair, so the payload is what a real block
/// would carry rather than what the validator insists on.
#[must_use]
pub fn block_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(COL_FINGERPRINT, DataType::UInt64, false),
        Field::new(COL_TIMESTAMP, DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]))
}

/// `series` distinct series, each with `samples` samples, as sorted batches.
///
/// Rows come out ordered by fingerprint and then timestamp, which is the sort
/// key `series_block_schema()` declares. Writing them in that order is what
/// gives the Parquet row groups disjoint fingerprint ranges, and so what makes
/// the block's statistics able to prune at all -- unsorted rows would produce
/// a block whose every row group spans every series, and a read benchmark over
/// that measures a scan rather than a lookup.
///
/// # Panics
/// Panics when the generated columns cannot form a batch, which would mean the
/// schema above and the arrays below have drifted apart.
#[must_use]
pub fn series_batches(series: usize, samples: usize) -> Vec<RecordBatch> {
    let schema = block_schema();
    let mut noise = Seeded::new(0x5EED_B10C);

    let mut fingerprints: Vec<u64> = (0..series)
        .map(|which| series_labels(which).fingerprint())
        .collect();
    fingerprints.sort_unstable();

    let mut batches = Vec::new();
    let mut fps: Vec<u64> = Vec::with_capacity(ROWS_PER_BATCH);
    let mut timestamps: Vec<i64> = Vec::with_capacity(ROWS_PER_BATCH);
    let mut values: Vec<f64> = Vec::with_capacity(ROWS_PER_BATCH);

    for fingerprint in fingerprints {
        for sample in 0..samples {
            fps.push(fingerprint);
            timestamps.push(
                i64::try_from(sample).expect("a sample index fits an i64") * SCRAPE_INTERVAL_MS,
            );
            values.push(noise.next_sample());
            if fps.len() == ROWS_PER_BATCH {
                batches.push(batch(&schema, &mut fps, &mut timestamps, &mut values));
            }
        }
    }
    if !fps.is_empty() {
        batches.push(batch(&schema, &mut fps, &mut timestamps, &mut values));
    }
    batches
}

/// The interval between two samples of the same series, in milliseconds.
const SCRAPE_INTERVAL_MS: i64 = 15_000;

/// One `RecordBatch` from the accumulated columns, which it drains.
fn batch(
    schema: &SchemaRef,
    fps: &mut Vec<u64>,
    timestamps: &mut Vec<i64>,
    values: &mut Vec<f64>,
) -> RecordBatch {
    let built = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(UInt64Array::from(std::mem::take(fps))),
            Arc::new(Int64Array::from(std::mem::take(timestamps))),
            Arc::new(Float64Array::from(std::mem::take(values))),
        ],
    )
    .expect("the generated columns match the block schema");
    fps.reserve(ROWS_PER_BATCH);
    timestamps.reserve(ROWS_PER_BATCH);
    values.reserve(ROWS_PER_BATCH);
    built
}

/// The label set of series `which`, for a caller that needs the labels too.
#[must_use]
pub fn labels_of(which: usize) -> Labels {
    series_labels(which)
}
