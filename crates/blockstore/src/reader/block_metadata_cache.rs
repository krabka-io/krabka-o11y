use super::{
    Arc, ByteSize, ByteSizeExt, CachedFileMetadataEntry, CachedParquetMetaData, FileMetadataCache,
    ObjectMeta, ParquetMetaData, same_object,
};

/// A bounded cache of Parquet footer metadata, keyed by object key and
/// validated against the object's current version on every read.
///
/// Every scan used to re-`head` each block, re-fetch its footer, and re-parse
/// it. The footer is the expensive part: two round trips and a Thrift decode,
/// repeated for each block of each query, and a range query walks the same
/// blocks once per step.
///
/// # Validation, not expiry
///
/// The cache never serves an entry it has not just checked. A read still
/// `head`s the object, and the entry is used only when the current
/// [`ObjectMeta`] still matches the one the entry was stored with — the `ETag`
/// and version where the backend supplies them, the size and modification time
/// otherwise. A key alone is not a safe cache key here: blocks are rewritten
/// in place when a delete request is materialized, and an entry keyed on the
/// key alone would then describe bytes that are no longer there. Losing the
/// `head` as well would need a time-to-live, which trades correctness for a
/// round trip; this trades nothing.
///
/// # Bound
///
/// Entries are held in `DataFusion`'s own byte-bounded LRU, the same store its
/// Parquet source reads and writes, under the same value type. Warming this
/// cache warms the scan, and one limit covers both. See
/// [`DEFAULT_BLOCK_METADATA_CACHE_MAX`](super::DEFAULT_BLOCK_METADATA_CACHE_MAX)
/// for the default and
/// [`BlockStore::with_metadata_cache_max`](crate::BlockStore::with_metadata_cache_max)
/// for setting it.
#[derive(Clone)]
pub struct BlockMetadataCache {
    entries: Arc<FileMetadataCache>,
}

impl BlockMetadataCache {
    pub(crate) fn new(entries: Arc<FileMetadataCache>) -> Self {
        Self { entries }
    }

    /// The cached footer metadata for the object `meta` describes, when the
    /// cache holds one and it still describes those bytes.
    ///
    /// An entry that fails validation is dropped on the way out, so a rewritten
    /// block does not hold its stale footer in the budget until the LRU gets
    /// round to it.
    #[must_use]
    pub fn get(&self, meta: &ObjectMeta) -> Option<Arc<ParquetMetaData>> {
        let entry = self.entries.get(&meta.location)?;
        if !same_object(&entry.meta, meta) {
            self.entries.remove(&meta.location);
            return None;
        }
        let cached = entry
            .file_metadata
            .as_any()
            .downcast_ref::<CachedParquetMetaData>()?;
        Some(Arc::clone(cached.parquet_metadata()))
    }

    /// Stores `metadata` as the footer of the object `meta` describes.
    pub fn put(&self, meta: &ObjectMeta, metadata: Arc<ParquetMetaData>) {
        self.entries.put(
            &meta.location,
            CachedFileMetadataEntry::new(
                meta.clone(),
                Arc::new(CachedParquetMetaData::new(metadata)),
            ),
        );
    }

    /// The cache's memory budget. Entries are evicted least-recently-used once
    /// the footers held exceed it.
    #[must_use]
    pub fn max_bytes(&self) -> ByteSize {
        ByteSize::from_bytes(self.entries.cache_limit() as u64)
    }

    /// How many blocks' footers the cache holds.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the cache holds nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Drops every entry.
    pub fn clear(&self) {
        self.entries.clear();
    }
}

impl std::fmt::Debug for BlockMetadataCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockMetadataCache")
            .field("entries", &self.entries.len())
            .field("max_bytes", &self.entries.cache_limit())
            .finish()
    }
}
