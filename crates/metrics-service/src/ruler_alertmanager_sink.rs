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
