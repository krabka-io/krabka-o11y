use super::*;

pub struct AlertmanagerHttpSink {
    pub(crate) client: reqwest::Client,
    pub(crate) endpoint: String,
}

impl AlertmanagerHttpSink {
    #[must_use]
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            endpoint: endpoint.into(),
        }
    }
}

#[async_trait::async_trait]
impl AlertmanagerSink for AlertmanagerHttpSink {
    async fn dispatch_alerts(
        &self,
        alerts: Vec<krabka_promql::AlertmanagerAlert>,
    ) -> Result<(), RulerWalError> {
        if alerts.is_empty() {
            return Ok(());
        }
        let response = self
            .client
            .post(&self.endpoint)
            .json(&alertmanager_payload(alerts))
            .send()
            .await
            .map_err(|error| RulerWalError::Append(error.to_string()))?;
        if !response.status().is_success() {
            return Err(RulerWalError::Append(format!(
                "alertmanager dispatch returned HTTP {}",
                response.status()
            )));
        }
        Ok(())
    }
}
