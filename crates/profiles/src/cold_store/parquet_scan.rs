//! Range-backed scans of profile samples. Decode work runs on a blocking worker.

use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::{array::BooleanArray, datatypes::DataType};
use futures::TryStreamExt;
use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::{ArrowPredicateFn, RowFilter},
        async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder},
    },
    file::{metadata::RowGroupMetaData, statistics::Statistics},
};

use super::{
    Arc, AsArray, BTreeMap, BTreeSet, BlockRowFilter, Int64Type, ObjectStore, Path, ProfileError,
    RecordBatch, SeriesFingerprint, UInt64Type, filter_and_remap_batch,
};

mod object_reader;

use self::object_reader::ObjectReader;

pub(super) struct ScanFilter {
    pub fingerprints: BTreeSet<SeriesFingerprint>,
    pub profile_type: Option<String>,
    pub start_ms: i64,
    pub end_ms: i64,
}

impl ScanFilter {
    fn overlaps(&self, group: &RowGroupMetaData) -> bool {
        if self.start_ms > self.end_ms || self.fingerprints.is_empty() {
            return false;
        }
        if let Some(Statistics::Int64(stats)) = group.column(1).statistics()
            && let (Some(min), Some(max)) = (stats.min_opt(), stats.max_opt())
            && (*max < self.start_ms || *min > self.end_ms)
        {
            return false;
        }
        if let Some(Statistics::Int64(stats)) = group.column(0).statistics()
            && let (Some(min), Some(max)) = (stats.min_opt(), stats.max_opt())
        {
            // Parquet stores UInt64 statistics in physical INT64 values.
            let min = u64::from_ne_bytes(min.to_ne_bytes());
            let max = u64::from_ne_bytes(max.to_ne_bytes());
            if min <= max && self.fingerprints.range(min..=max).next().is_none() {
                return false;
            }
        }
        true
    }

    fn row_filter(&self, projection: ProjectionMask, decoded_rows: Arc<AtomicUsize>) -> RowFilter {
        let start_ms = self.start_ms;
        let end_ms = self.end_ms;
        let fingerprints = self.fingerprints.clone();
        let profile_type = self.profile_type.clone();
        let predicate = ArrowPredicateFn::new(projection, move |batch: RecordBatch| {
            decoded_rows.fetch_add(batch.num_rows(), Ordering::Relaxed);
            let fps = batch.column(0).as_primitive::<UInt64Type>();
            let timestamps = batch.column(1).as_primitive::<Int64Type>();
            let types = profile_type
                .as_ref()
                .map(|_| arrow::compute::cast(batch.column(2), &DataType::Utf8))
                .transpose()?;
            Ok(BooleanArray::from_iter((0..batch.num_rows()).map(|row| {
                fingerprints.contains(&fps.value(row))
                    && timestamps.value(row) >= start_ms
                    && timestamps.value(row) <= end_ms
                    && profile_type.as_ref().is_none_or(|expected| {
                        types
                            .as_ref()
                            .is_some_and(|types| types.as_string::<i32>().value(row) == expected)
                    })
            })))
        });
        RowFilter::new(vec![Box::new(predicate)])
    }
}

pub(super) async fn load(
    store: Arc<dyn ObjectStore>,
    path: Path,
    filter: ScanFilter,
    partition_map: Option<BTreeMap<u64, u64>>,
) -> Result<Vec<RecordBatch>, ProfileError> {
    let block_key = path.to_string();
    let runtime = tokio::runtime::Handle::current();
    let reader = ObjectReader::new(store, path);
    let fetched_bytes = Arc::clone(&reader.fetched_bytes);
    // Poll the scan here so footer, predicate, and payload decoding stay off async workers.
    tokio::task::spawn_blocking(move || {
        runtime
            .block_on(scan(reader, filter, partition_map))
            .map(|scan| scan.batches)
    })
    .await
    .map_err(|err| ProfileError::Store(err.to_string()))?
    .inspect(|_| {
        tracing::debug!(
            block_key,
            fetched_bytes = fetched_bytes.load(Ordering::Relaxed),
            "cold profile Parquet ranges"
        );
    })
}

struct ScanResult {
    batches: Vec<RecordBatch>,
    predicate_rows: usize,
}

async fn scan<T: AsyncFileReader + Unpin + 'static>(
    reader: T,
    filter: ScanFilter,
    partition_map: Option<BTreeMap<u64, u64>>,
) -> Result<ScanResult, ProfileError> {
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|err| ProfileError::Store(err.to_string()))?;
    let groups = builder
        .metadata()
        .row_groups()
        .iter()
        .enumerate()
        .filter_map(|(index, group)| filter.overlaps(group).then_some(index))
        .collect();
    let metadata_only = partition_map.is_none();
    let predicate_projection = ProjectionMask::roots(
        builder.parquet_schema(),
        0..if filter.profile_type.is_some() { 3 } else { 2 },
    );
    let projection = if metadata_only {
        ProjectionMask::roots(builder.parquet_schema(), [0, 1])
    } else {
        ProjectionMask::all()
    };
    let predicate_rows = Arc::new(AtomicUsize::new(0));
    let row_filter = filter.row_filter(predicate_projection, Arc::clone(&predicate_rows));
    let mut stream = builder
        .with_row_groups(groups)
        .with_projection(projection)
        .with_row_filter(row_filter)
        .build()
        .map_err(|err| ProfileError::Store(err.to_string()))?;
    let mut batches = Vec::new();
    while let Some(batch) = stream
        .try_next()
        .await
        .map_err(|err| ProfileError::Store(err.to_string()))?
    {
        let batch = if let Some(map) = &partition_map {
            filter_and_remap_batch(
                &batch,
                map,
                BlockRowFilter {
                    fps: &filter.fingerprints,
                    profile_type: filter.profile_type.as_deref().unwrap_or_default(),
                    start_ms: filter.start_ms,
                    end_ms: filter.end_ms,
                },
            )?
        } else {
            batch
        };
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
    }
    let result = ScanResult {
        batches,
        predicate_rows: predicate_rows.load(Ordering::Relaxed),
    };
    tracing::debug!(
        predicate_rows = result.predicate_rows,
        output_rows = result
            .batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        metadata_only,
        "cold profile Parquet scan"
    );
    Ok(result)
}

#[cfg(test)]
mod tests;
