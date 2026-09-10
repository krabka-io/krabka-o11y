use super::{
    Arc, ArrowReaderOptions, AsyncFileReader, BoxFuture, Bytes, FutureExt, GetOptions, ObjectMeta,
    ObjectStore, ParquetMetaData, ParquetMetaDataReader, Path, Range, to_parquet_error,
    try_join_all,
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
    e_tag: Option<String>,
    version: Option<String>,
}

impl BlockObjectReader {
    pub(crate) fn new(store: Arc<dyn ObjectStore>, meta: ObjectMeta) -> Self {
        Self {
            store,
            path: meta.location,
            size: meta.size,
            e_tag: meta.e_tag,
            version: meta.version,
        }
    }

    fn get_options(&self, range: Range<u64>) -> GetOptions {
        GetOptions {
            if_match: self.e_tag.clone(),
            range: Some(range.into()),
            version: self.version.clone(),
            ..GetOptions::default()
        }
    }
}

impl AsyncFileReader for BlockObjectReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        async move {
            let result = self
                .store
                .get_opts(&self.path, self.get_options(range))
                .await
                .map_err(to_parquet_error)?;
            result.bytes().await.map_err(to_parquet_error)
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        async move {
            try_join_all(ranges.into_iter().map(|range| {
                let store = Arc::clone(&self.store);
                let path = self.path.clone();
                let options = self.get_options(range);
                async move {
                    let result = store.get_opts(&path, options).await?;
                    result.bytes().await
                }
            }))
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

#[cfg(test)]
mod tests {
    use object_store::{ObjectStoreExt, PutPayload, memory::InMemory};

    use super::*;

    #[tokio::test]
    async fn every_range_is_pinned_to_the_headed_object() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("block.parquet");
        store
            .put(&path, PutPayload::from_static(b"parquet"))
            .await
            .unwrap();
        let mut meta = store.head(&path).await.unwrap();
        meta.e_tag = Some("etag".to_string());
        meta.version = Some("version".to_string());
        let reader = BlockObjectReader::new(store, meta);

        for range in [0..2, 2..7] {
            let options = reader.get_options(range.clone());
            assert2::assert!(options.if_match.as_deref() == Some("etag"));
            assert2::assert!(options.version.as_deref() == Some("version"));
            assert2::assert!(options.range == Some(range.into()));
        }
    }
}
