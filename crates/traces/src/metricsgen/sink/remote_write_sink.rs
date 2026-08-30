use super::{SeriesPayload, SinkError, async_trait};

/// Output edge for Prometheus `remote_write` payloads.
#[async_trait]
pub trait RemoteWriteSink: Send + Sync {
    async fn write(&self, payload: &SeriesPayload) -> Result<(), SinkError>;
}
