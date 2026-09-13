use krabka_blockstore::{Labels, TenantId};

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

    async fn dispatch_alerts_for_tenant(
        &self,
        _tenant: &TenantId,
        alerts: Vec<AlertmanagerAlert>,
    ) -> Result<(), RulerWalError> {
        self.dispatch_alerts(alerts).await
    }
}
