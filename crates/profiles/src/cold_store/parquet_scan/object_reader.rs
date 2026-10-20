use std::{ops::Range, sync::atomic::Ordering};

use bytes::Bytes;
use futures::{FutureExt, TryFutureExt, future::BoxFuture};
use object_store::{GetOptions, GetRange, ObjectStoreExt};
use parquet::{
    arrow::{
        arrow_reader::ArrowReaderOptions,
        async_reader::{AsyncFileReader, MetadataSuffixFetch},
    },
    errors::{ParquetError, Result},
    file::metadata::{ParquetMetaData, ParquetMetaDataReader},
};

use super::{Arc, AtomicUsize, ObjectStore, Path};

pub(super) struct ObjectReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
    pub fetched_bytes: Arc<AtomicUsize>,
}

impl ObjectReader {
    pub fn new(store: Arc<dyn ObjectStore>, path: Path) -> Self {
        Self {
            store,
            path,
            fetched_bytes: Arc::default(),
        }
    }
}

fn parquet_error(error: object_store::Error) -> ParquetError {
    ParquetError::External(Box::new(error))
}

impl AsyncFileReader for ObjectReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        async move {
            let bytes = self
                .store
                .get_range(&self.path, range)
                .await
                .map_err(parquet_error)?;
            self.fetched_bytes.fetch_add(bytes.len(), Ordering::Relaxed);
            Ok(bytes)
        }
        .boxed()
    }

    fn get_byte_ranges(&mut self, ranges: Vec<Range<u64>>) -> BoxFuture<'_, Result<Vec<Bytes>>> {
        async move {
            let bytes = self
                .store
                .get_ranges(&self.path, &ranges)
                .await
                .map_err(parquet_error)?;
            self.fetched_bytes
                .fetch_add(bytes.iter().map(Bytes::len).sum(), Ordering::Relaxed);
            Ok(bytes)
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>>> {
        async move {
            ParquetMetaDataReader::new()
                .with_arrow_reader_options(options)
                .load_via_suffix_and_finish(self)
                .await
                .map(Arc::new)
        }
        .boxed()
    }
}

impl MetadataSuffixFetch for &mut ObjectReader {
    fn fetch_suffix(&mut self, suffix: usize) -> BoxFuture<'_, Result<Bytes>> {
        let options = GetOptions {
            range: Some(GetRange::Suffix(suffix as u64)),
            ..Default::default()
        };
        async move {
            let bytes = self
                .store
                .get_opts(&self.path, options)
                .await
                .map_err(parquet_error)?
                .bytes()
                .map_err(parquet_error)
                .await?;
            self.fetched_bytes.fetch_add(bytes.len(), Ordering::Relaxed);
            Ok(bytes)
        }
        .boxed()
    }
}
