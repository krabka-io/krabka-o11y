use super::{
    Arc, CompactionIndexError, CompactionIndexManifest, CompactionIndexSink, ObjectStore,
    ObjectStoreExt, Path, PutPayload, async_trait};

/// Object-store backed compaction index sidecar sink.
pub struct ObjectStoreCompactionIndexSink {
    pub(crate) store: Arc<dyn ObjectStore>,
}

impl ObjectStoreCompactionIndexSink {
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    /// Reads and decodes a compaction index manifest written earlier.
    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub async fn read_manifest(
        &self,
        index_key: &str,
    ) -> Result<CompactionIndexManifest, CompactionIndexError> {
        let bytes = self
            .store
            .get(&Path::from(index_key))
            .await
            .map_err(|error| CompactionIndexError::ObjectStore(error.to_string()))?
            .bytes()
            .await
            .map_err(|error| CompactionIndexError::ObjectStore(error.to_string()))?;
        CompactionIndexManifest::decode(&bytes)
    }
}

#[async_trait]
impl CompactionIndexSink for ObjectStoreCompactionIndexSink {
    async fn write_manifest(
        &self,
        manifest: &CompactionIndexManifest,
    ) -> Result<(), CompactionIndexError> {
        let bytes = manifest.encode()?;
        self.store
            .put(
                &Path::from(manifest.index_key.clone()),
                PutPayload::from(bytes),
            )
            .await
            .map_err(|error| CompactionIndexError::ObjectStore(error.to_string()))?;
        Ok(())
    }
}
