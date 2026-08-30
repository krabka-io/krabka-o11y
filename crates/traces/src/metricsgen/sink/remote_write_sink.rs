use super::{async_trait, SeriesPayload, SinkError};

/// Output edge for Prometheus `remote_write` payloads.
#[async_trait]
pub trait RemoteWriteSink: Send + Sync {
    async fn write(&self, payload: &SeriesPayload) -> Result<(), SinkError>;
}
