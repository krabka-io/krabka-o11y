//! Writes columnar blocks to object storage as Parquet.

use std::{collections::BTreeSet, sync::Arc};

use arrow::{
    array::{Array, FixedSizeBinaryArray, Int64Array, UInt64Array},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use object_store::{ObjectStore, buffered::BufWriter, path::Path};
use parquet::arrow::AsyncArrowWriter;
use tracing::instrument;

use crate::{
    block::{BlockMeta, COL_FINGERPRINT, COL_TIMESTAMP, validate_against},
    block_index::{BlockSchema, series_block_schema},
    error::{BlockStoreError, Result},
    labels::SeriesFingerprint,
};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int64Array, StringArray, UInt64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};

    use super::*;

    fn log_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(crate::COL_FINGERPRINT, DataType::UInt64, false),
            Field::new(crate::COL_TIMESTAMP, DataType::Int64, false),
            Field::new("line", DataType::Utf8, true),
        ]))
    }

    fn sample_batch(schema: &Arc<Schema>) -> RecordBatch {
        let fp = UInt64Array::from(vec![10_u64, 10, 20, 20]);
        let ts = Int64Array::from(vec![100_i64, 200, 300, 400]);
        let line = StringArray::from(vec!["a", "b", "c", "d"]);
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(fp), Arc::new(ts), Arc::new(line)],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn write_block_persists_object_and_returns_meta() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = BlockWriter::new(store.clone());
        let schema = log_schema();
        let batch = sample_batch(&schema);

        let meta = writer
            .write_block("tenant-a", "blocks/tenant-a/b1.parquet", schema, &[batch])
            .await
            .unwrap();

        let mut meta = meta;
        meta.fingerprints.sort_unstable();
        assert2::assert!(
            meta == BlockMeta {
                tenant: "tenant-a".to_string(),
                object_key: "blocks/tenant-a/b1.parquet".to_string(),
                min_ts: 100,
                max_ts: 400,
                row_count: 4,
                fingerprints: vec![10, 20],
            }
        );

        let head = store.head(&Path::from("blocks/tenant-a/b1.parquet")).await;
        assert2::assert!(head.is_ok());
    }

    fn span_summary_batch() -> RecordBatch {
        use arrow::array::FixedSizeBinaryArray;

        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::FixedSizeBinary(16), false),
            Field::new("start_unix_nano", DataType::Int64, false),
        ]));
        let ids: Vec<[u8; 16]> = vec![[1_u8; 16], [2_u8; 16]];
        let trace_id =
            FixedSizeBinaryArray::try_from_iter(ids.iter().map(<[u8; 16]>::as_slice)).unwrap();
        let ts = Int64Array::from(vec![100_i64, 200]);
        RecordBatch::try_new(schema, vec![Arc::new(trace_id), Arc::new(ts)]).unwrap()
    }

    #[test]
    fn summarize_skips_fingerprints_for_span_blocks() {
        // Span (FixedSizeBinary id) blocks never read `meta.fingerprints`, so
        // the per-row FNV pass should be skipped and the set left empty. Time
        // bounds and row count must still be summarized.
        let batch = span_summary_batch();
        let (min_ts, max_ts, row_count, fps) = summarize(
            &[batch],
            &SummaryColumns::new("trace_id", "start_unix_nano"),
        )
        .unwrap();
        assert2::assert!(min_ts == 100);
        assert2::assert!(max_ts == 200);
        assert2::assert!(row_count == 2);
        assert2::assert!(fps.is_empty());
    }

    #[test]
    fn summarize_still_fingerprints_series_blocks() {
        let schema = log_schema();
        let batch = sample_batch(&schema);
        let (_min, _max, _rows, mut fps) = summarize(&[batch], &SummaryColumns::series()).unwrap();
        fps.sort_unstable();
        assert2::assert!(fps == vec![10_u64, 20]);
    }

    #[tokio::test]
    async fn write_block_rejects_schema_without_mandatory_columns() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = BlockWriter::new(store);
        let schema = Arc::new(Schema::new(vec![Field::new("line", DataType::Utf8, true)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(vec!["x"]))])
                .unwrap();

        let err = writer.write_block("t", "k.parquet", schema, &[batch]).await;
        assert2::assert!(err.is_err());
    }

    #[tokio::test]
    async fn write_block_rejects_batch_schema_mismatch() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = BlockWriter::new(store);
        let schema = log_schema();
        let batch_schema = Arc::new(Schema::new(vec![
            Field::new(crate::COL_TIMESTAMP, DataType::Int64, false),
            Field::new(crate::COL_FINGERPRINT, DataType::UInt64, false),
            Field::new("line", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            batch_schema,
            vec![
                Arc::new(Int64Array::from(vec![100_i64])),
                Arc::new(UInt64Array::from(vec![10_u64])),
                Arc::new(StringArray::from(vec!["x"])),
            ],
        )
        .unwrap();

        let err = writer.write_block("t", "k.parquet", schema, &[batch]).await;

        assert2::assert!(
            matches!(err, Err(BlockStoreError::InvalidBlock(message)) if message.contains("schema"))
        );
    }
}

// === split-modules: generated submodules ===
mod block_writer;
mod summarize;
mod summary_columns;
mod validate_batch_schemas;

pub use block_writer::BlockWriter;
use summarize::summarize;
pub use summary_columns::SummaryColumns;
use validate_batch_schemas::validate_batch_schemas;
