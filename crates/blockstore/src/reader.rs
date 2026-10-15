//! Reads Parquet blocks back from object storage into Arrow `RecordBatch`es.

use std::{ops::Range, sync::Arc};

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use datafusion::{
    datasource::physical_plan::parquet::metadata::CachedParquetMetaData,
    execution::cache::cache_manager::{CachedFileMetadataEntry, FileMetadataCache},
};
use futures::{FutureExt, TryFutureExt, TryStreamExt, future::BoxFuture};
use krabka_units::prelude::*;
use object_store::{GetOptions, GetRange, ObjectMeta, ObjectStore, ObjectStoreExt, path::Path};
use parquet::{
    arrow::{
        ParquetRecordBatchStreamBuilder,
        arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions},
        async_reader::{AsyncFileReader, MetadataSuffixFetch},
    },
    errors::ParquetError,
    file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader},
};
use tracing::instrument;

use crate::error::{BlockReadFailure, BlockStoreError, Result};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int64Array, StringArray, UInt64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use object_store::{
        ObjectMeta, ObjectStore, PutPayload, buffered::BufWriter, memory::InMemory, path::Path,
    };
    use parquet::{arrow::AsyncArrowWriter, file::properties::WriterProperties};

    use super::*;

    #[derive(Debug, Eq, PartialEq)]
    struct RowGroupStatus {
        index: usize,
        compressed: bool,
    }

    struct ChangedObjectCase {
        description: &'static str,
        changed: ObjectMeta,
    }

    #[derive(Debug, Eq, PartialEq)]
    struct BlockReadSummary {
        object_key: String,
        skip_reason: Option<crate::BlockSkipReason>,
        missing: bool,
    }
    use crate::{log_line_schema::log_line_schema, writer::BlockWriter};

    async fn write_test_log_block(store: Arc<dyn ObjectStore>) -> RecordBatch {
        let batch = two_line_batch(["x", "y"]);
        BlockWriter::new(store)
            .write_block(
                "t",
                "b.parquet",
                log_line_schema(),
                std::slice::from_ref(&batch),
            )
            .await
            .unwrap();
        batch
    }

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
        let batch = write_test_log_block(store.clone()).await;

        let out = read_block(store, "b.parquet").await.unwrap();
        assert2::assert!(out == vec![batch]);
    }

    #[tokio::test]
    async fn a_missing_or_future_block_format_is_rejected_before_rows_are_read() {
        struct Case {
            name: &'static str,
            marker: Option<&'static str>,
            message: &'static str,
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new(crate::COL_FINGERPRINT, DataType::UInt64, false),
            Field::new(crate::COL_TIMESTAMP, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![10_u64])),
                Arc::new(Int64Array::from(vec![100_i64])),
            ],
        )
        .unwrap();
        let cases = [
            Case {
                name: "a block without a version marker",
                marker: None,
                message: "unsupported persisted block format version: the marker is missing",
            },
            Case {
                name: "a block of a previous version",
                marker: Some("1"),
                message: "unsupported persisted block format version `1`",
            },
            Case {
                name: "a block of a future version",
                marker: Some("3"),
                message: "unsupported persisted block format version `3`",
            },
        ];
        for case in cases {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let metadata = case.marker.map(|version| {
                vec![parquet::file::metadata::KeyValue::new(
                    crate::PERSISTED_BLOCK_FORMAT_KEY.to_string(),
                    Some(version.to_string()),
                )]
            });
            let props = WriterProperties::builder()
                .set_key_value_metadata(metadata)
                .build();
            let object_writer = BufWriter::new(store.clone(), Path::from("block.parquet"));
            let mut writer =
                AsyncArrowWriter::try_new(object_writer, schema.clone(), Some(props)).unwrap();
            writer.write(&batch).await.unwrap();
            writer.close().await.unwrap();

            let error = read_block(store, "block.parquet")
                .await
                .expect_err("the block must not be decoded");
            assert2::check!(
                matches!(&error, BlockStoreError::InvalidBlock(message) if message == case.message),
                "{}: {error}",
                case.name
            );
        }
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
        let schema = log_line_schema();
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
        let _batch = write_test_log_block(store.clone()).await;

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
        let batch = two_line_batch(["first", "second"]);

        // One row per row group → exactly two row groups.
        write_one_row_per_group(&store, "meta.parquet", &batch).await;

        let meta = read_row_group_metadata(store.clone(), "meta.parquet")
            .await
            .unwrap();
        let project = |metadata: &[RowGroupMeta]| {
            metadata
                .iter()
                .map(|group| RowGroupStatus {
                    index: group.index,
                    compressed: group.compressed > ByteSize::ZERO,
                })
                .collect::<Vec<_>>()
        };
        assert2::assert!(
            project(&meta)
                == vec![
                    RowGroupStatus {
                        index: 0,
                        compressed: true
                    },
                    RowGroupStatus {
                        index: 1,
                        compressed: true
                    },
                ]
        );

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
        assert2::assert!(
            project(&meta)
                == vec![
                    RowGroupStatus {
                        index: 0,
                        compressed: true
                    },
                    RowGroupStatus {
                        index: 1,
                        compressed: true
                    },
                ]
        );
    }

    fn two_line_batch(lines: [&str; 2]) -> RecordBatch {
        RecordBatch::try_new(
            log_line_schema(),
            vec![
                Arc::new(UInt64Array::from(vec![10_u64, 20])),
                Arc::new(Int64Array::from(vec![100_i64, 200])),
                Arc::new(StringArray::from(lines.to_vec())),
            ],
        )
        .unwrap()
    }

    // One row per row group, so a two-row batch makes exactly two groups.
    async fn write_one_row_per_group(store: &Arc<dyn ObjectStore>, key: &str, batch: &RecordBatch) {
        let object_writer = BufWriter::new(store.clone(), Path::from(key));
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(1))
            .set_write_batch_size(1)
            .set_key_value_metadata(Some(vec![parquet::file::metadata::KeyValue::new(
                crate::PERSISTED_BLOCK_FORMAT_KEY.to_string(),
                Some(crate::PERSISTED_BLOCK_FORMAT_VERSION.to_string()),
            )]))
            .build();
        let mut writer =
            AsyncArrowWriter::try_new(object_writer, batch.schema(), Some(props)).unwrap();
        writer.write(batch).await.unwrap();
        writer.close().await.unwrap();
    }

    fn empty_cache() -> BlockMetadataCache {
        BlockMetadataCache::new(Arc::new(
            datafusion::execution::cache::default_cache::DefaultCache::new(1024 * 1024),
        ))
    }

    async fn write_test_block(store: &Arc<dyn ObjectStore>, key: &str, lines: &[&str]) {
        let schema = log_line_schema();
        let rows = i64::try_from(lines.len()).unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from_iter_values(std::iter::repeat_n(
                    7_u64,
                    lines.len(),
                ))),
                Arc::new(Int64Array::from_iter_values(0..rows)),
                Arc::new(StringArray::from(lines.to_vec())),
            ],
        )
        .unwrap();
        BlockWriter::new(store.clone())
            .write_block("t", key, schema, &[batch])
            .await
            .unwrap();
    }

    /// A read of one named block knows which block it was reading, so the key
    /// belongs in the error rather than in a message the caller has to parse
    /// back out — and the backend error has to survive whole, or a caller
    /// cannot tell a missing object from a store that is down.
    #[tokio::test]
    async fn an_unreadable_block_names_itself_and_keeps_its_cause() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        write_test_block(&store, "present.parquet", &["x"]).await;
        store
            .put(
                &Path::from("garbage.parquet"),
                PutPayload::from(b"PAR1 not really a parquet block".to_vec()),
            )
            .await
            .unwrap();
        // A real block's leading bytes, cut short: the footer it points at is
        // not there.
        let whole = store
            .get(&Path::from("present.parquet"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        store
            .put(
                &Path::from("truncated.parquet"),
                PutPayload::from(whole[..whole.len() / 2].to_vec()),
            )
            .await
            .unwrap();

        let mut got = Vec::new();
        for key in ["absent.parquet", "garbage.parquet", "truncated.parquet"] {
            let error = read_row_group_metadata(store.clone(), key)
                .await
                .expect_err("an unreadable block is an error");
            let unreadable = error
                .unreadable_block()
                .expect("the error is about one block");
            got.push(BlockReadSummary {
                object_key: unreadable.object_key.to_string(),
                skip_reason: unreadable.failure.skip_reason(),
                missing: unreadable.failure.is_missing(),
            });
        }

        assert2::assert!(
            got == [
                BlockReadSummary {
                    object_key: "absent.parquet".to_string(),
                    skip_reason: Some(crate::BlockSkipReason::Missing),
                    missing: true,
                },
                BlockReadSummary {
                    object_key: "garbage.parquet".to_string(),
                    skip_reason: Some(crate::BlockSkipReason::Corrupt),
                    missing: false,
                },
                BlockReadSummary {
                    object_key: "truncated.parquet".to_string(),
                    skip_reason: Some(crate::BlockSkipReason::Corrupt),
                    missing: false,
                },
            ]
        );
    }

    /// The second read of a block's footer is served from the cache: same
    /// `Arc`, no second decode.
    #[tokio::test]
    async fn a_cached_footer_is_read_once() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        write_test_block(&store, "b.parquet", &["x", "y"]).await;
        let cache = empty_cache();

        let first = block_metadata(&store, "b.parquet", DEFAULT_BLOCK_READ_MAX, Some(&cache))
            .await
            .unwrap();
        let second = block_metadata(&store, "b.parquet", DEFAULT_BLOCK_READ_MAX, Some(&cache))
            .await
            .unwrap();

        assert2::assert!(cache.len() == 1);
        assert2::assert!(Arc::ptr_eq(&first, &second));

        // Without a cache nothing is remembered, and each read decodes afresh.
        let uncached = block_metadata(&store, "b.parquet", DEFAULT_BLOCK_READ_MAX, None)
            .await
            .unwrap();
        assert2::assert!(!Arc::ptr_eq(&first, &uncached));
    }

    /// The validator is the point of the key. An entry whose object no longer
    /// matches is not served, and is not left occupying the budget either.
    #[tokio::test]
    async fn a_changed_object_invalidates_its_entry() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        write_test_block(&store, "b.parquet", &["x", "y"]).await;
        let cache = empty_cache();
        let metadata = block_metadata(&store, "b.parquet", DEFAULT_BLOCK_READ_MAX, Some(&cache))
            .await
            .unwrap();
        let meta = store.head(&Path::from("b.parquet")).await.unwrap();
        assert2::assert!(cache.get(&meta).is_some());

        let cases = [
            ChangedObjectCase {
                description: "a different size",
                changed: ObjectMeta {
                    size: meta.size + 1,
                    ..meta.clone()
                },
            },
            ChangedObjectCase {
                description: "a different etag",
                changed: ObjectMeta {
                    e_tag: Some("rewritten".to_string()),
                    ..meta.clone()
                },
            },
            ChangedObjectCase {
                description: "a different version",
                changed: ObjectMeta {
                    version: Some("2".to_string()),
                    ..meta.clone()
                },
            },
        ];
        for case in cases {
            cache.put(&meta, Arc::clone(&metadata));
            assert2::assert!(cache.get(&case.changed).is_none(), "{}", case.description);
            // The stale entry is dropped rather than left to the LRU.
            assert2::assert!(cache.is_empty(), "{}", case.description);
        }
    }

    /// Rewriting a block in place — which materializing a delete request does
    /// — must not be answered from the footer of the bytes that used to be
    /// there.
    #[tokio::test]
    async fn a_rewritten_block_is_read_afresh() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        write_test_block(&store, "b.parquet", &["x", "y"]).await;
        let cache = empty_cache();
        let before = block_metadata(&store, "b.parquet", DEFAULT_BLOCK_READ_MAX, Some(&cache))
            .await
            .unwrap();

        write_test_block(&store, "b.parquet", &["a", "b", "c", "d"]).await;
        let after = block_metadata(&store, "b.parquet", DEFAULT_BLOCK_READ_MAX, Some(&cache))
            .await
            .unwrap();

        assert2::assert!(before.file_metadata().num_rows() == 2);
        assert2::assert!(after.file_metadata().num_rows() == 4);
    }

    #[test]
    fn the_footer_cache_bound_is_a_documented_default() {
        assert2::assert!(DEFAULT_BLOCK_METADATA_CACHE_MAX == mebibytes(128));
    }

    #[tokio::test]
    async fn read_block_row_groups_reads_only_selected_groups() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let batch = two_line_batch(["first", "second"]);

        write_one_row_per_group(&store, "rg.parquet", &batch).await;

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
mod block_metadata_cache;
mod object_store_reader;
pub use block_metadata_cache::BlockMetadataCache;
use object_store_reader::ObjectStoreReader;

/// Default memory budget for the Parquet footer cache of one [`BlockStore`](crate::BlockStore).
pub const DEFAULT_BLOCK_METADATA_CACHE_MAX: ByteSize = mebibytes(128);

/// Maximum on-disk byte size of a Parquet block accepted by [`read_block`](crate::read_block).
pub const DEFAULT_BLOCK_READ_MAX: ByteSize = gibibytes(1);

/// How much of a block's tail one footer read asks for.
const FOOTER_PREFETCH: ByteSize = kibibytes(64);

/// One block's cache participation: the object as `head` just reported it, and
/// the cache to validate against and fill.
#[derive(Clone, Debug)]
pub(crate) struct CachedBlock {
    meta: ObjectMeta,
    cache: BlockMetadataCache,
}

/// One readable block, as a probe found it.
#[derive(Clone, Debug)]
pub(crate) struct ProbedBlock {
    /// The object key the index named.
    pub(crate) object_key: String,
    /// The object as the probe's `head` described it.
    pub(crate) meta: ObjectMeta,
    /// The decoded footer.
    pub(crate) metadata: Arc<ParquetMetaData>,
}

/// Minimal row-group metadata used by query frontends to shard block scans.
#[derive(Clone, Debug, PartialEq)]
pub struct RowGroupMeta {
    pub index: usize,
    pub compressed: ByteSize,
}

async fn collect_block_batches(
    reader: ObjectStoreReader,
    object_key: &str,
    row_groups: Option<&[usize]>,
) -> Result<Vec<RecordBatch>> {
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|error| unreadable(object_key, error))?;
    crate::validate_persisted_block_format(builder.metadata())
        .map_err(crate::BlockStoreError::InvalidBlock)?;
    let stream = match row_groups {
        Some(row_groups) => builder.with_row_groups(row_groups.to_vec()).build(),
        None => builder.build(),
    }
    .map_err(|error| unreadable(object_key, error))?;
    stream
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| unreadable(object_key, error))
}

pub(crate) fn to_parquet_error(error: object_store::Error) -> ParquetError {
    ParquetError::External(Box::new(error))
}

/// Reads one block's Parquet footer, through `cache` when one is supplied.
///
/// See [`probe_block`] for what the read costs and why.
///
/// # Errors
/// See [`probe_block`].
pub(crate) async fn block_metadata(
    store: &Arc<dyn ObjectStore>,
    object_key: &str,
    max_bytes: ByteSize,
    cache: Option<&BlockMetadataCache>,
) -> Result<Arc<ParquetMetaData>> {
    probe_block(store, object_key, max_bytes, cache)
        .await
        .map(|probed| probed.metadata)
}

/// Reads every `RecordBatch` from the Parquet block at `object_key`.
///
/// The reader rejects the block with an error when its on-disk size exceeds
/// [`DEFAULT_BLOCK_READ_MAX`], before it streams any bytes.
///
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn read_block(store: Arc<dyn ObjectStore>, object_key: &str) -> Result<Vec<RecordBatch>> {
    read_block_with_max_bytes(store, object_key, DEFAULT_BLOCK_READ_MAX).await
}

/// Reads selected row groups from a Parquet block.
///
/// As with [`read_block`](crate::read_block), the reader rejects the block when its on-disk size
/// exceeds [`DEFAULT_BLOCK_READ_MAX`], before it streams any bytes.
///
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn read_block_row_groups(
    store: Arc<dyn ObjectStore>,
    object_key: &str,
    row_groups: &[usize],
) -> Result<Vec<RecordBatch>> {
    read_block_row_groups_with_max_bytes(store, object_key, row_groups, DEFAULT_BLOCK_READ_MAX)
        .await
}

/// Reads row-group sizes from Parquet metadata and does not scan row data.
///
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn read_row_group_metadata(
    store: Arc<dyn ObjectStore>,
    object_key: &str,
) -> Result<Vec<RowGroupMeta>> {
    read_row_group_metadata_with_max_bytes(store, object_key, DEFAULT_BLOCK_READ_MAX).await
}

/// Reads row-group sizes with a caller-supplied on-disk size limit.
///
/// Reads through no cache. A [`BlockStore`](crate::BlockStore) has one and uses
/// it; this free function has nothing to hang one on.
///
/// # Errors
/// Returns an error when object-store I/O fails, the block exceeds
/// `max_bytes`, or persisted metadata is malformed.
#[instrument(
    level = "debug",
    skip_all,
    fields(object_key = %object_key, size = tracing::field::Empty),
    err
)]
pub async fn read_row_group_metadata_with_max_bytes(
    store: Arc<dyn ObjectStore>,
    object_key: &str,
    max_bytes: ByteSize,
) -> Result<Vec<RowGroupMeta>> {
    row_group_metadata(&store, object_key, max_bytes, None).await
}

/// Reads row-group sizes from a block's footer, through `cache` when supplied.
///
/// # Errors
/// See [`block_metadata`].
pub(crate) async fn row_group_metadata(
    store: &Arc<dyn ObjectStore>,
    object_key: &str,
    max_bytes: ByteSize,
    cache: Option<&BlockMetadataCache>,
) -> Result<Vec<RowGroupMeta>> {
    let metadata = block_metadata(store, object_key, max_bytes, cache).await?;
    Ok((0..metadata.row_groups().len())
        .map(|index| {
            let row_group = &metadata.row_groups()[index];
            RowGroupMeta {
                index,
                compressed: ByteSize::from_bytes(
                    u64::try_from(row_group.compressed_size()).unwrap_or(0),
                ),
            }
        })
        .collect())
}

/// Whether two [`ObjectMeta`]s describe the same bytes at the same key.
///
/// Every field the backend can offer as a validator has to agree. Where the
/// store supplies an `ETag` or a version, that settles it; where it does not,
/// both sides are `None` and the size and modification time carry the check.
/// The comparison is deliberately conservative: a false negative costs one
/// footer read, and a false positive answers a query from the wrong bytes.
pub(crate) fn same_object(cached: &ObjectMeta, current: &ObjectMeta) -> bool {
    cached.location == current.location
        && cached.size == current.size
        && cached.last_modified == current.last_modified
        && cached.e_tag == current.e_tag
        && cached.version == current.version
}

/// Attributes a backend error to the block it was read from.
///
/// Every function here reads one named block, so a failure below it is that
/// block's failure, and the key belongs in the error rather than in a message
/// the caller would have to parse back out.
///
/// This takes the error rather than returning a closure over the key on
/// purpose. A returned `impl Fn(..)` is an opaque type carrying the key's
/// lifetime, and holding one across an `.await` leaves the whole future
/// `Send` only for one specific lifetime — which breaks every `async fn` up
/// the call chain.
pub(crate) fn unreadable<E>(object_key: &str, error: E) -> BlockStoreError
where
    E: Into<BlockReadFailure>,
{
    BlockStoreError::block_unreadable(object_key, error.into())
}

/// `head`s the block, rejects it above `max_bytes`, and hands back the object
/// as the store currently describes it.
///
/// The returned [`ObjectMeta`] carries the size the Parquet reader needs and
/// the `ETag`, version and modification time the footer cache validates against,
/// so the one `head` a read already paid for serves both.
///
/// A failed `head` is attributed to the block: a missing object is one block's
/// problem, and the caller has to be able to tell it from a store that is
/// down. An over-cap block is not — the block is readable and the store is
/// fine, and a query that quietly dropped a legitimately large block would
/// hide more data than it reported.
pub(crate) async fn head_within_cap(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    object_key: &str,
    max_bytes: ByteSize,
) -> Result<ObjectMeta> {
    let meta = store
        .head(path)
        .await
        .map_err(|error| unreadable(object_key, error))?;
    tracing::Span::current().record("size", meta.size);
    if ByteSize::from_bytes(meta.size) > max_bytes {
        return Err(BlockStoreError::InvalidBlock(format!(
            "block `{object_key}` is {} bytes, exceeds cap of {} bytes",
            meta.size,
            max_bytes.bytes_u64()
        )));
    }
    Ok(meta)
}

/// `head`s one block and reads its Parquet footer, through `cache` when one is
/// supplied.
///
/// This is the cheapest thing that proves a block is readable: it `head`s the
/// object and decodes the footer, and never touches a column chunk. A scan
/// uses it to find the blocks it cannot read *before* it hands a file list to
/// `DataFusion`, where one bad file fails the whole plan.
///
/// The `head` runs on a cache hit too. It is what finds a block that deletion
/// removed after the index named it, and it is what validates a cached footer
/// against a block that was rewritten in place. See [`BlockMetadataCache`].
///
/// On a cache miss the footer read is one bounded range of
/// [`FOOTER_PREFETCH`] bytes at the end of the object. With a cache the read
/// includes the page index, so the cache holds everything that the
/// `DataFusion` Parquet reader asks for, and a scan of the block reads only
/// column chunks.
///
/// # Errors
/// Returns [`BlockStoreError::BlockUnreadable`](crate::BlockStoreError::BlockUnreadable)
/// when the object is missing or is not a decodable Parquet block, or when the
/// store fails; the error carries the backend error whole, so the caller can
/// tell those apart. Returns
/// [`BlockStoreError::InvalidBlock`](crate::BlockStoreError::InvalidBlock)
/// when the block is larger than `max_bytes`.
#[instrument(
    level = "debug",
    skip_all,
    fields(object_key = %object_key, size = tracing::field::Empty, cached = tracing::field::Empty),
    err
)]
pub(crate) async fn probe_block(
    store: &Arc<dyn ObjectStore>,
    object_key: &str,
    max_bytes: ByteSize,
    cache: Option<&BlockMetadataCache>,
) -> Result<ProbedBlock> {
    let path = Path::from(object_key);
    let meta = head_within_cap(store, &path, object_key, max_bytes).await?;
    if let Some(cache) = cache
        && let Some(metadata) = cache.get(&meta)
    {
        crate::validate_persisted_block_format(&metadata)
            .map_err(crate::BlockStoreError::InvalidBlock)?;
        tracing::Span::current().record("cached", true);
        return Ok(ProbedBlock {
            object_key: object_key.to_string(),
            meta,
            metadata,
        });
    }
    tracing::Span::current().record("cached", false);

    // Only a cached footer reaches the `DataFusion` scan, which reads the page
    // index. A caller without a cache reads the footer for its own use.
    let page_index = if cache.is_some() {
        PageIndexPolicy::Optional
    } else {
        PageIndexPolicy::Skip
    };
    let mut reader = ObjectStoreReader::new(Arc::clone(store), path);
    let metadata = ParquetMetaDataReader::new()
        .with_prefetch_hint(Some(FOOTER_PREFETCH.bytes_usize()))
        .with_page_index_policy(page_index)
        .load_and_finish(&mut reader, meta.size)
        .await
        .map_err(|error| unreadable(object_key, error))?;
    let metadata = Arc::new(metadata);
    // The check that a Parquet reader makes before it reads a row: the
    // Parquet schema must convert to an Arrow schema. A block that fails it is
    // corrupt here, where a scan can still skip it, and not in `DataFusion`.
    ArrowReaderMetadata::try_new(Arc::clone(&metadata), ArrowReaderOptions::new())
        .map_err(|error| unreadable(object_key, error))?;
    crate::validate_persisted_block_format(&metadata)
        .map_err(crate::BlockStoreError::InvalidBlock)?;
    if let Some(cache) = cache {
        cache.put(&meta, Arc::clone(&metadata));
    }
    Ok(ProbedBlock {
        object_key: object_key.to_string(),
        meta,
        metadata,
    })
}

/// Reads selected row groups, reading the block's footer through `cache` when
/// one is supplied.
///
/// # Errors
/// Returns an error when object-store I/O fails, the block exceeds
/// `max_bytes`, persisted metadata is malformed, or the block cannot be
/// decoded.
#[instrument(
    level = "debug",
    skip_all,
    fields(object_key = %object_key, row_groups = row_groups.len(), size = tracing::field::Empty),
    err
)]
pub(crate) async fn read_block_row_groups_cached(
    store: &Arc<dyn ObjectStore>,
    object_key: &str,
    row_groups: &[usize],
    max_bytes: ByteSize,
    cache: Option<&BlockMetadataCache>,
) -> Result<Vec<RecordBatch>> {
    let path = Path::from(object_key);
    let meta = head_within_cap(store, &path, object_key, max_bytes).await?;
    let reader = match cache {
        Some(cache) => ObjectStoreReader::with_cache(
            Arc::clone(store),
            CachedBlock {
                meta,
                cache: cache.clone(),
            },
        ),
        None => ObjectStoreReader::new(Arc::clone(store), path),
    };
    collect_block_batches(reader, object_key, Some(row_groups)).await
}

/// Reads selected row groups with a caller-supplied on-disk size limit.
///
/// Reads through no cache; see
/// [`read_row_group_metadata_with_max_bytes`](read_row_group_metadata_with_max_bytes).
///
/// # Errors
/// Returns an error when object-store I/O fails, the block exceeds
/// `max_bytes`, persisted metadata is malformed, or the block cannot be
/// decoded.
pub async fn read_block_row_groups_with_max_bytes(
    store: Arc<dyn ObjectStore>,
    object_key: &str,
    row_groups: &[usize],
    max_bytes: ByteSize,
) -> Result<Vec<RecordBatch>> {
    read_block_row_groups_cached(&store, object_key, row_groups, max_bytes, None).await
}

/// Reads every `RecordBatch` with a caller-supplied on-disk size limit.
///
/// # Errors
/// Returns an error when object-store I/O fails, the block exceeds
/// `max_bytes`, persisted metadata is malformed, or the block cannot be
/// decoded.
#[instrument(
    level = "debug",
    skip_all,
    fields(object_key = %object_key, size = tracing::field::Empty),
    err
)]
pub async fn read_block_with_max_bytes(
    store: Arc<dyn ObjectStore>,
    object_key: &str,
    max_bytes: ByteSize,
) -> Result<Vec<RecordBatch>> {
    let path = Path::from(object_key);
    head_within_cap(&store, &path, object_key, max_bytes).await?;
    let reader = ObjectStoreReader::new(store, path);
    collect_block_batches(reader, object_key, None).await
}
