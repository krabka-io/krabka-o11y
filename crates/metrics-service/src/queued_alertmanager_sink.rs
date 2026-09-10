use super::{AlertmanagerHttpSink, AlertmanagerSink, RulerWalError};

/// A bounded FIFO in front of Alertmanager delivery.
///
/// Enqueueing is the evaluation-path success boundary. When the queue is full,
/// producers wait for capacity instead of dropping alerts; the worker retries
/// each batch until one configured endpoint accepts it.
#[derive(Clone)]
pub struct QueuedAlertmanagerSink {
    sender: std::sync::Arc<
        tokio::sync::Mutex<
            Option<tokio::sync::mpsc::Sender<Vec<krabka_promql::AlertmanagerAlert>>>,
        >,
    >,
    worker: std::sync::Arc<tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
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
        let worker = tokio::spawn(async move {
            while let Some(alerts) = receiver.recv().await {
                loop {
                    match sink.deliver(alerts.clone()).await {
                        Ok(()) => break,
                        Err(error) if error.is_retryable() => {
                            tracing::warn!(%error, "alertmanager delivery failed; retrying queued batch");
                            tokio::time::sleep(resend_delay).await;
                        }
                        Err(error) => {
                            tracing::error!(%error, "alertmanager rejected queued batch");
                            break;
                        }
                    }
                }
            }
        });
        Self {
            sender: std::sync::Arc::new(tokio::sync::Mutex::new(Some(sender))),
            worker: std::sync::Arc::new(tokio::sync::Mutex::new(Some(worker))),
        }
    }

    /// Close the queue and wait until the worker delivers all accepted batches.
    ///
    /// # Errors
    /// Returns an error if the drain times out or the worker does not complete.
    pub async fn shutdown(&self, drain_timeout: std::time::Duration) -> Result<(), RulerWalError> {
        self.sender.lock().await.take();
        if let Some(mut worker) = self.worker.lock().await.take() {
            if let Ok(result) = tokio::time::timeout(drain_timeout, &mut worker).await {
                result.map_err(|error| {
                    RulerWalError::Append(format!("alertmanager delivery worker failed: {error}"))
                })?;
            } else {
                worker.abort();
                let _ = worker.await;
                return Err(RulerWalError::Append(
                    "alertmanager delivery queue drain timed out".to_string(),
                ));
            }
        }
        Ok(())
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
        let sender = self.sender.lock().await.clone().ok_or_else(|| {
            RulerWalError::Append("alertmanager delivery queue stopped".to_string())
        })?;
        sender
            .send(alerts)
            .await
            .map_err(|_| RulerWalError::Append("alertmanager delivery queue stopped".to_string()))
    }
}
