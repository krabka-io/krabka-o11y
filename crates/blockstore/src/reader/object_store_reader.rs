use super::{
    Arc, ArrowReaderOptions, AsyncFileReader, BoxFuture, Bytes, CachedBlock, FutureExt, GetOptions,
    GetRange, MetadataSuffixFetch, ObjectStore, ObjectStoreExt, ParquetMetaData,
    ParquetMetaDataReader, Path, Range, TryFutureExt, to_parquet_error,
};

/// A Parquet byte source over one object, optionally memoising its footer.
///
/// `cached` is `None` for the free functions, which have no store to hang a
/// cache on, and `Some` for reads a [`BlockStore`](crate::BlockStore) mediates.
/// Only the footer is memoised: column chunks are large, are read once per
/// query, and are already pruned by the predicate, so caching them would spend
/// the whole budget on bytes the next query does not want.
#[derive(Clone, Debug)]
pub(crate) struct ObjectStoreReader {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) path: Path,
    pub(crate) cached: Option<CachedBlock>,
}

impl ObjectStoreReader {
    pub(crate) fn new(store: Arc<dyn ObjectStore>, path: Path) -> Self {
        Self {
            store,
            path,
            cached: None,
        }
    }

    /// A reader that consults and fills `cached.cache` for this block's footer.
    pub(crate) fn with_cache(store: Arc<dyn ObjectStore>, cached: CachedBlock) -> Self {
        Self {
            store,
            path: cached.meta.location.clone(),
            cached: Some(cached),
        }
    }
}

impl AsyncFileReader for ObjectStoreReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.store
            .get_range(&self.path, range)
            .map_err(to_parquet_error)
            .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        async move {
            self.store
                .get_ranges(&self.path, &ranges)
                .await
                .map_err(to_parquet_error)
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        let cached = self.cached.clone();
        async move {
            if let Some(cached) = &cached
                && let Some(metadata) = cached.cache.get(&cached.meta)
            {
                return Ok(metadata);
            }
            let metadata = ParquetMetaDataReader::new()
                .with_arrow_reader_options(options)
                .load_via_suffix_and_finish(self)
                .await?;
            let metadata = Arc::new(metadata);
            if let Some(cached) = &cached {
                cached.cache.put(&cached.meta, Arc::clone(&metadata));
            }
            Ok(metadata)
        }
        .boxed()
    }
}

impl MetadataSuffixFetch for &mut ObjectStoreReader {
    fn fetch_suffix(&mut self, suffix: usize) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let options = GetOptions {
            range: Some(GetRange::Suffix(suffix as u64)),
            ..Default::default()
        };
        async move {
            let result = self
                .store
                .get_opts(&self.path, options)
                .await
                .map_err(to_parquet_error)?;
            result.bytes().await.map_err(to_parquet_error)
        }
        .boxed()
    }
}
