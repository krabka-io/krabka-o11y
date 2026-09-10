use super::{AlertmanagerHttpSink, AlertmanagerSink, RulerWalError};

/// A bounded FIFO in front of Alertmanager delivery.
///
/// Enqueueing is the evaluation-path success boundary. When the queue is full,
/// producers wait for capacity instead of dropping alerts; the worker retries
/// each batch until one configured endpoint accepts it.
pub struct QueuedAlertmanagerSink {
    sender: tokio::sync::mpsc::Sender<Vec<krabka_promql::AlertmanagerAlert>>,
}

impl QueuedAlertmanagerSink {
    #[must_use]
    pub fn new(
        sink: AlertmanagerHttpSink,
        capacity: usize,
        resend_delay: std::time::Duration,
    ) -> Self {
        let (sender, mut receiver) =
            tokio::sync::mpsc::channel::<Vec<krabka_promql::AlertmanagerAlert>>(capacity.max(1));
        tokio::spawn(async move {
            while let Some(alerts) = receiver.recv().await {
                loop {
                    match sink.dispatch_alerts(alerts.clone()).await {
                        Ok(()) => break,
                        Err(error) => {
                            tracing::warn!(%error, "alertmanager delivery failed; retrying queued batch");
                            tokio::time::sleep(resend_delay).await;
                        }
                    }
                }
            }
        });
        Self { sender }
    }
}

#[async_trait::async_trait]
impl AlertmanagerSink for QueuedAlertmanagerSink {
    async fn dispatch_alerts(
        &self,
        alerts: Vec<krabka_promql::AlertmanagerAlert>,
    ) -> Result<(), RulerWalError> {
        if alerts.is_empty() {
            return Ok(());
        }
        self.sender
            .send(alerts)
            .await
            .map_err(|_| RulerWalError::Append("alertmanager delivery queue stopped".to_string()))
    }
}
