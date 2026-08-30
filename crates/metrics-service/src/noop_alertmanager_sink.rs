use super::{AlertmanagerSink, RulerWalError};

#[derive(Clone, Copy, Debug, Default)]
pub struct NoopAlertmanagerSink;

#[async_trait::async_trait]
impl AlertmanagerSink for NoopAlertmanagerSink {
    async fn dispatch_alerts(
        &self,
        alerts: Vec<krabka_promql::AlertmanagerAlert>,
    ) -> Result<(), RulerWalError> {
        if !alerts.is_empty() {
            tracing::warn!(
                alert_count = alerts.len(),
                "ruler alertmanager sink is not configured; dropping alerts"
            );
        }
        Ok(())
    }
}
