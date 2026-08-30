use super::{AlertmanagerAlert, RulerWalError};

/// Sink for firing alert notifications.
#[async_trait::async_trait]
pub trait AlertmanagerSink: Send + Sync {
    async fn dispatch_alerts(&self, alerts: Vec<AlertmanagerAlert>) -> Result<(), RulerWalError>;
}
