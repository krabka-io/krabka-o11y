//! Reads Parquet blocks back from object storage into Arrow `RecordBatch`es.

use std::{ops::Range, sync::Arc};

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures::{FutureExt, TryFutureExt, TryStreamExt, future::BoxFuture};
use krabka_units::prelude::*;
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt, path::Path};
use parquet::{
    arrow::{
        ParquetRecordBatchStreamBuilder,
        arrow_reader::ArrowReaderOptions,
        async_reader::{AsyncFileReader, MetadataSuffixFetch},
    },
    errors::ParquetError,
    file::metadata::{ParquetMetaData, ParquetMetaDataReader},
};
use tracing::instrument;

use crate::error::{BlockStoreError, Result};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int64Array, StringArray, UInt64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use object_store::{
        ObjectStore, PutPayload, buffered::BufWriter, memory::InMemory, path::Path,
    };
    use parquet::{arrow::AsyncArrowWriter, file::properties::WriterProperties};

    use super::*;
    use crate::writer::BlockWriter;

    #[test]
    fn max_block_bytes_is_one_gib() {
        assert2::assert!(DEFAULT_BLOCK_READ_MAX == gibibytes(1));
        assert2::assert!(DEFAULT_BLOCK_READ_MAX.bytes_u64() == 1024 * 1024 * 1024);
    }

    #[tokio::test]
    async fn metadata_suffix_fetch_reads_only_the_requested_tail() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("suffix");
        store
            .put(&path, PutPayload::from(b"metadata-tail".to_vec()))
            .await
            .unwrap();

        let mut reader = ObjectStoreReader::new(store, path);
        let suffix = (&mut reader).fetch_suffix(4).await.unwrap();

        assert2::assert!(suffix == Bytes::from_static(b"tail"));
    }

    #[tokio::test]
    async fn write_then_read_round_trips_rows() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let schema = Arc::new(Schema::new(vec![
            Field::new(crate::COL_FINGERPRINT, DataType::UInt64, false),
            Field::new(crate::COL_TIMESTAMP, DataType::Int64, false),
            Field::new("line", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![10_u64, 20])),
                Arc::new(Int64Array::from(vec![100_i64, 200])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .unwrap();

        BlockWriter::new(store.clone())
            .write_block("t", "b.parquet", schema, std::slice::from_ref(&batch))
            .await
            .unwrap();

        let out = read_block(store, "b.parquet").await.unwrap();
        assert2::assert!(out == vec![batch]);
    }

    /// `read_block_row_groups` is the default-cap wrapper the query path calls,
    /// and nothing exercised it -- only the `_with_max_bytes` form beneath it.
    /// Replaced by `Ok(vec![])` it reports every block as holding no rows, which
    /// reads as an empty time range rather than as a failure.
    ///
    /// The rows read back are what pins it: `write_block` coalesces the batches
    /// it is given into a single row group, so there is only group 0 to select,
    /// and the distinguishing observation is that it holds both rows.
    #[tokio::test]
    async fn read_block_row_groups_returns_the_selected_group() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let schema = Arc::new(Schema::new(vec![
            Field::new(crate::COL_FINGERPRINT, DataType::UInt64, false),
            Field::new(crate::COL_TIMESTAMP, DataType::Int64, false),
            Field::new("line", DataType::Utf8, true),
        ]));
        let group = |fp: u64, ts: i64, line: &str| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(UInt64Array::from(vec![fp])),
                    Arc::new(Int64Array::from(vec![ts])),
                    Arc::new(StringArray::from(vec![line])),
                ],
            )
            .unwrap()
        };
        let first = group(10, 100, "first");
        let second = group(20, 200, "second");

        BlockWriter::new(store.clone())
            .write_block("t", "b.parquet", schema, &[first.clone(), second.clone()])
            .await
            .unwrap();

        // `write_block` coalesces the batches it is given into one row group,
        // so group 0 is the only one there is. Reading it back must yield both
        // rows, which an empty return does not.
        let rows = read_block_row_groups(store.clone(), "b.parquet", &[0])
            .await
            .unwrap();
        assert2::check!(rows.iter().map(RecordBatch::num_rows).sum::<usize>() == 2);

        // Selecting nothing is distinct from the wrapper answering nothing:
        // both are empty here, so the assertion above is what separates them.
        let none = read_block_row_groups(store, "b.parquet", &[])
            .await
            .unwrap();
        assert2::check!(none.iter().map(RecordBatch::num_rows).sum::<usize>() == 0);
    }

    #[tokio::test]
    async fn read_block_with_max_bytes_rejects_over_cap_block() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let schema = Arc::new(Schema::new(vec![
            Field::new(crate::COL_FINGERPRINT, DataType::UInt64, false),
            Field::new(crate::COL_TIMESTAMP, DataType::Int64, false),
            Field::new("line", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![10_u64, 20])),
                Arc::new(Int64Array::from(vec![100_i64, 200])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .unwrap();

        BlockWriter::new(store.clone())
            .write_block("t", "b.parquet", schema, &[batch])
            .await
            .unwrap();

        // A tiny cap stands in for the production cap so the test need not
        // materialize an over-cap block; the real block is well above 1 byte.
        let got =
            read_block_with_max_bytes(store.clone(), "b.parquet", ByteSize::from_bytes(1)).await;
        assert2::assert!(got.is_err());

        // A cap exactly at the real size is accepted; only bytes above the cap
        // are rejected.
        let size = store.head(&Path::from("b.parquet")).await.unwrap().size;
        let out = read_block_with_max_bytes(store, "b.parquet", ByteSize::from_bytes(size))
            .await
            .unwrap();
        let total: usize = out.iter().map(RecordBatch::num_rows).sum();
        assert2::assert!(total == 2);
    }

    #[tokio::test]
    async fn read_row_group_metadata_reports_every_group() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let schema = Arc::new(Schema::new(vec![
            Field::new(crate::COL_FINGERPRINT, DataType::UInt64, false),
            Field::new(crate::COL_TIMESTAMP, DataType::Int64, false),
            Field::new("line", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![10_u64, 20])),
                Arc::new(Int64Array::from(vec![100_i64, 200])),
                Arc::new(StringArray::from(vec!["first", "second"])),
            ],
        )
        .unwrap();

        // One row per row group → exactly two row groups.
        let object_writer = BufWriter::new(store.clone(), Path::from("meta.parquet"));
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(1))
            .set_write_batch_size(1)
            .build();
        let mut writer =
            AsyncArrowWriter::try_new(object_writer, schema.clone(), Some(props)).unwrap();
        writer.write(&batch).await.unwrap();
        writer.close().await.unwrap();

        let meta = read_row_group_metadata(store.clone(), "meta.parquet")
            .await
            .unwrap();
        let project = |metadata: &[RowGroupMeta]| {
            metadata
                .iter()
                .map(|group| (group.index, group.compressed > ByteSize::ZERO))
                .collect::<Vec<_>>()
        };
        assert2::assert!(project(&meta) == vec![(0, true), (1, true)]);

        let got = read_row_group_metadata_with_max_bytes(
            store.clone(),
            "meta.parquet",
            ByteSize::from_bytes(1),
        )
        .await;
        assert2::assert!(got.is_err());

        let size = store.head(&Path::from("meta.parquet")).await.unwrap().size;
        let meta = read_row_group_metadata_with_max_bytes(
            store,
            "meta.parquet",
            ByteSize::from_bytes(size),
        )
        .await
        .unwrap();
        assert2::assert!(project(&meta) == vec![(0, true), (1, true)]);
    }

    #[tokio::test]
    async fn read_block_row_groups_reads_only_selected_groups() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let schema = Arc::new(Schema::new(vec![
            Field::new(crate::COL_FINGERPRINT, DataType::UInt64, false),
            Field::new(crate::COL_TIMESTAMP, DataType::Int64, false),
            Field::new("line", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![10_u64, 20])),
                Arc::new(Int64Array::from(vec![100_i64, 200])),
                Arc::new(StringArray::from(vec!["first", "second"])),
            ],
        )
        .unwrap();

        let object_writer = BufWriter::new(store.clone(), Path::from("rg.parquet"));
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(1))
            .set_write_batch_size(1)
            .build();
        let mut writer =
            AsyncArrowWriter::try_new(object_writer, schema.clone(), Some(props)).unwrap();
        writer.write(&batch).await.unwrap();
        writer.close().await.unwrap();

        let got = read_block_row_groups_with_max_bytes(
            store.clone(),
            "rg.parquet",
            &[1],
            ByteSize::from_bytes(1),
        )
        .await;
        assert2::assert!(got.is_err());

        let size = store.head(&Path::from("rg.parquet")).await.unwrap().size;
        let out = read_block_row_groups_with_max_bytes(
            store,
            "rg.parquet",
            &[1],
            ByteSize::from_bytes(size),
        )
        .await
        .unwrap();

        let total: usize = out.iter().map(RecordBatch::num_rows).sum();
        let lines = out[0]
            .column_by_name("line")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert2::assert!(total == 1);
        assert2::assert!(lines.value(0) == "second");
    }
}

mod default_block_read_max;
mod head_within_cap;
mod object_store_reader;
mod read_block;
mod read_block_row_groups;
mod read_block_row_groups_with_max_bytes;
mod read_block_with_max_bytes;
mod read_row_group_metadata;
mod read_row_group_metadata_with_max_bytes;
mod row_group_meta;
mod to_parquet_error;

pub use default_block_read_max::DEFAULT_BLOCK_READ_MAX;
use head_within_cap::head_within_cap;
use object_store_reader::ObjectStoreReader;
pub use read_block::read_block;
pub use read_block_row_groups::read_block_row_groups;
pub use read_block_row_groups_with_max_bytes::read_block_row_groups_with_max_bytes;
pub use read_block_with_max_bytes::read_block_with_max_bytes;
pub use read_row_group_metadata::read_row_group_metadata;
pub use read_row_group_metadata_with_max_bytes::read_row_group_metadata_with_max_bytes;
pub use row_group_meta::RowGroupMeta;
use to_parquet_error::to_parquet_error;
