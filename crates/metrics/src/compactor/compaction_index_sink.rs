use super::*;

/// Sink for compaction index sidecars.
#[async_trait]
pub trait CompactionIndexSink: Send + Sync {
    async fn write_manifest(
        &self,
        manifest: &CompactionIndexManifest,
    ) -> Result<(), CompactionIndexError>;
}
