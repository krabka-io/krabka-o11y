use krabka_blockstore::Labels;

use super::{AlertmanagerAlert, RulerWalError};

/// Sink for firing alert notifications.
#[async_trait::async_trait]
pub trait AlertmanagerSink: Send + Sync {
    fn template_external_labels(&self) -> Labels {
        Labels::new()
    }

    fn template_external_url(&self, _alert_name: &str) -> String {
        String::new()
    }

    async fn dispatch_alerts(&self, alerts: Vec<AlertmanagerAlert>) -> Result<(), RulerWalError>;
}
