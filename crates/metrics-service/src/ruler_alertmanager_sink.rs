use super::{AlertmanagerHttpSink, AlertmanagerSink, NoopAlertmanagerSink, RulerWalError};

pub enum RulerAlertmanagerSink {
    Http(AlertmanagerHttpSink),
    Noop(NoopAlertmanagerSink),
}

impl RulerAlertmanagerSink {
    #[must_use]
    pub fn from_endpoint(endpoint: Option<String>) -> Self {
        endpoint.map_or(Self::Noop(NoopAlertmanagerSink), |endpoint| {
            Self::Http(AlertmanagerHttpSink::new(endpoint))
        })
    }

    #[must_use]
    pub fn from_endpoints(
        endpoints: Vec<String>,
        external_labels: std::collections::BTreeMap<String, String>,
        generator_url_template: Option<String>,
    ) -> Self {
        if endpoints.is_empty() {
            Self::Noop(NoopAlertmanagerSink)
        } else {
            Self::Http(AlertmanagerHttpSink::with_delivery(
                endpoints,
                external_labels,
                generator_url_template,
                3,
                std::time::Duration::from_millis(250),
            ))
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
