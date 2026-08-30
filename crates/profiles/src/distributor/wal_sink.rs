use super::*;

#[async_trait::async_trait]
pub trait WalSink: Send + Sync {
    async fn append(&self, rec: ProfileRecord) -> Result<(), ProfilesError>;
}
