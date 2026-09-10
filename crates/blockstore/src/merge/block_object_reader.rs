use super::{
    Arc, ArrowReaderOptions, AsyncFileReader, BoxFuture, Bytes, FutureExt, ObjectStore,
    ObjectStoreExt, ParquetMetaData, ParquetMetaDataReader, Path, Range, to_parquet_error,
};

/// A Parquet byte source over one block on an object store.
///
/// The Parquet crate's own `ParquetObjectReader` is deprecated in favour of
/// callers implementing [`AsyncFileReader`] themselves, and this is the
/// implementation a merge needs: one block, read once, front to back. It keeps
/// no footer cache, unlike the reader a query goes through -- a compaction
/// reads each of its inputs exactly once, so there is nothing for a cache to
/// save -- and it takes the object's size from the `head` the caller already
/// paid for rather than going back to the store to ask.
pub(crate) struct BlockObjectReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
    size: u64,
}

impl BlockObjectReader {
    pub(crate) fn new(store: Arc<dyn ObjectStore>, path: Path, size: u64) -> Self {
        Self { store, path, size }
    }
}

impl AsyncFileReader for BlockObjectReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        async move {
            self.store
                .get_range(&self.path, range)
                .await
                .map_err(to_parquet_error)
        }
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
        let size = self.size;
        async move {
            ParquetMetaDataReader::new()
                .with_arrow_reader_options(options)
                .load_and_finish(self, size)
                .await
                .map(Arc::new)
        }
        .boxed()
    }
}
