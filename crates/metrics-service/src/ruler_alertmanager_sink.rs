use super::{
    AlertmanagerHttpSink, AlertmanagerSink, NoopAlertmanagerSink, QueuedAlertmanagerSink,
    RulerWalError,
};

#[derive(Clone)]
pub enum RulerAlertmanagerSink {
    Http(QueuedAlertmanagerSink),
    Noop(NoopAlertmanagerSink),
}

impl RulerAlertmanagerSink {
    #[must_use]
    pub fn from_endpoint(endpoint: Option<String>) -> Self {
        endpoint.map_or(Self::Noop(NoopAlertmanagerSink), |endpoint| {
            Self::Http(QueuedAlertmanagerSink::new(
                AlertmanagerHttpSink::new(endpoint),
                64,
                std::time::Duration::from_secs(5),
            ))
        })
    }

    #[must_use]
    pub fn from_endpoints(
        endpoints: Vec<String>,
        external_labels: std::collections::BTreeMap<String, String>,
        generator_url_template: Option<String>,
        queue_capacity: usize,
    ) -> Self {
        if endpoints.is_empty() {
            Self::Noop(NoopAlertmanagerSink)
        } else {
            Self::Http(QueuedAlertmanagerSink::new(
                AlertmanagerHttpSink::with_delivery(
                    endpoints,
                    external_labels,
                    generator_url_template,
                    3,
                    std::time::Duration::from_millis(250),
                    std::time::Duration::from_secs(5),
                ),
                queue_capacity,
                std::time::Duration::from_secs(5),
            ))
        }
    }

    /// Close and drain the Alertmanager delivery queue.
    ///
    /// # Errors
    /// Returns an error if the drain times out or the worker does not complete.
    pub async fn shutdown(&self) -> Result<(), RulerWalError> {
        match self {
            Self::Http(sink) => sink.shutdown(std::time::Duration::from_secs(30)).await,
            Self::Noop(_) => Ok(()),
        }
    }
}

#[async_trait::async_trait]
impl AlertmanagerSink for RulerAlertmanagerSink {
    async fn dispatch_alerts(
        &self,
        alerts: Vec<krabka_promql::AlertmanagerAlert>,
    ) -> Result<(), RulerWalError> {
        match self {
            Self::Http(sink) => sink.dispatch_alerts(alerts).await,
            Self::Noop(sink) => sink.dispatch_alerts(alerts).await,
        }
    }
}
