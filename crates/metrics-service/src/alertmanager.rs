//! Alertmanager request delivery and ruler sinks.

use super::{AlertmanagerSink, BTreeMap, Duration, RulerWalError, unix_ms_to_rfc3339};

pub(crate) struct AlertmanagerDeliveryError {
    message: String,
    retryable: bool,
}

impl AlertmanagerDeliveryError {
    pub(crate) fn is_retryable(&self) -> bool {
        self.retryable
    }
}

impl std::fmt::Display for AlertmanagerDeliveryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

/// The generator URL of an alert: `template` with `{alertname}` filled in,
/// or empty when no template is configured.
pub(crate) fn generator_url_for(template: Option<&str>, alert_name: &str) -> String {
    template.map_or_else(String::new, |template| {
        template.replace("{alertname}", &encode_url_component(alert_name))
    })
}

pub(crate) fn encode_url_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(byte));
        } else {
            use std::fmt::Write as _;
            write!(encoded, "%{byte:02X}").expect("writing to a String cannot fail");
        }
    }
    encoded
}

/// The external labels and generator URL template an Alertmanager sink
/// stamps on the alerts it sends and exposes to rule templates.
#[derive(Clone)]
pub(crate) struct AlertTemplateDefaults {
    pub(crate) external_labels: BTreeMap<String, String>,
    pub(crate) generator_url_template: Option<String>,
}

impl AlertTemplateDefaults {
    pub(crate) fn external_labels(&self) -> krabka_blockstore::Labels {
        krabka_blockstore::Labels::from_pairs(
            self.external_labels
                .iter()
                .map(|pair| krabka_blockstore::LabelPair::new(pair.0.clone(), pair.1.clone())),
        )
    }

    pub(crate) fn external_url(&self, alert_name: &str) -> String {
        generator_url_for(self.generator_url_template.as_deref(), alert_name)
    }
}

pub struct AlertmanagerHttpSink {
    pub(crate) client: reqwest::Client,
    pub(crate) endpoints: Vec<String>,
    pub(crate) alert_templates: AlertTemplateDefaults,
    pub(crate) max_attempts: usize,
    pub(crate) retry_delay: Duration,
    pub(crate) request_timeout: Duration,
}

impl AlertmanagerHttpSink {
    #[must_use]
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self::with_delivery(
            vec![endpoint.into()],
            BTreeMap::new(),
            None,
            3,
            Duration::from_millis(250),
            Duration::from_secs(5),
        )
    }

    #[must_use]
    pub fn with_delivery(
        endpoints: Vec<String>,
        external_labels: BTreeMap<String, String>,
        generator_url_template: Option<String>,
        max_attempts: usize,
        retry_delay: Duration,
        request_timeout: Duration,
    ) -> Self {
        Self {
            client: reqwest::Client::new(),
            endpoints,
            alert_templates: AlertTemplateDefaults {
                external_labels,
                generator_url_template,
            },
            max_attempts: max_attempts.max(1),
            retry_delay,
            request_timeout,
        }
    }

    fn enrich(&self, alerts: &mut [krabka_promql::AlertmanagerAlert]) {
        for alert in alerts {
            for label in &self.alert_templates.external_labels {
                alert
                    .labels
                    .entry(label.0.clone())
                    .or_insert_with(|| label.1.clone());
            }
            if alert.generator_url.is_empty()
                && let Some(template) = &self.alert_templates.generator_url_template
            {
                let alert_name = alert.labels.get("alertname").map_or("", String::as_str);
                let alert_name = encode_url_component(alert_name);
                alert.generator_url = template.replace("{alertname}", &alert_name);
            }
        }
    }

    pub(crate) async fn deliver(
        &self,
        tenant: Option<&str>,
        mut alerts: Vec<krabka_promql::AlertmanagerAlert>,
    ) -> Result<(), AlertmanagerDeliveryError> {
        if alerts.is_empty() {
            return Ok(());
        }
        self.enrich(&mut alerts);
        let payload = alertmanager_payload(alerts);
        let mut failures = Vec::new();
        let mut retryable = false;
        for indexed_endpoint in self.endpoints.iter().enumerate() {
            let endpoint_index = indexed_endpoint.0;
            let endpoint = indexed_endpoint.1;
            for attempt in 1..=self.max_attempts {
                let mut request = self.client.post(endpoint).timeout(self.request_timeout);
                if let Some(tenant) = tenant {
                    request = request.header("X-Scope-OrgID", tenant);
                }
                match request.json(&payload).send().await {
                    Ok(response) if response.status().is_success() => return Ok(()),
                    Ok(response) => {
                        let status = response.status();
                        failures.push(format!("endpoint {}: HTTP {status}", endpoint_index + 1));
                        if status.is_client_error()
                            && status != reqwest::StatusCode::REQUEST_TIMEOUT
                            && status != reqwest::StatusCode::TOO_MANY_REQUESTS
                        {
                            break;
                        }
                        retryable = true;
                    }
                    Err(error) => {
                        retryable |= !error.is_builder();
                        failures.push(format!("endpoint {}: request failed", endpoint_index + 1));
                        if error.is_builder() {
                            break;
                        }
                    }
                }
                if attempt < self.max_attempts {
                    tokio::time::sleep(self.retry_delay).await;
                }
            }
        }
        Err(AlertmanagerDeliveryError {
            message: format!(
                "all alertmanager endpoints failed after bounded retries: {}",
                failures.join("; ")
            ),
            retryable,
        })
    }
}

impl AlertmanagerHttpSink {
    async fn dispatch_batch(
        &self,
        tenant: Option<&str>,
        alerts: Vec<krabka_promql::AlertmanagerAlert>,
    ) -> Result<(), RulerWalError> {
        self.deliver(tenant, alerts)
            .await
            .map_err(|error| RulerWalError::Append(error.to_string()))
    }
}

/// Implements [`AlertmanagerSink`] for a sink type that has an
/// `alert_templates: AlertTemplateDefaults` field and an inherent
/// `async fn dispatch_batch(&self, Option<&str>, Vec<AlertmanagerAlert>)`.
///
/// A blanket impl over a local helper trait is not possible because
/// `AlertmanagerSink` is foreign to this crate.
macro_rules! impl_alertmanager_sink {
    ($sink:ty) => {
        #[async_trait::async_trait]
        impl AlertmanagerSink for $sink {
            fn template_external_labels(&self) -> krabka_blockstore::Labels {
                self.alert_templates.external_labels()
            }

            fn template_external_url(&self, alert_name: &str) -> String {
                self.alert_templates.external_url(alert_name)
            }

            async fn dispatch_alerts(
                &self,
                alerts: Vec<krabka_promql::AlertmanagerAlert>,
            ) -> Result<(), RulerWalError> {
                self.dispatch_batch(None, alerts).await
            }

            async fn dispatch_alerts_for_tenant(
                &self,
                tenant: &krabka_blockstore::TenantId,
                alerts: Vec<krabka_promql::AlertmanagerAlert>,
            ) -> Result<(), RulerWalError> {
                self.dispatch_batch(Some(tenant.as_str()), alerts).await
            }
        }
    };
}
impl_alertmanager_sink!(AlertmanagerHttpSink);

pub(crate) fn alertmanager_payload(
    alerts: Vec<krabka_promql::AlertmanagerAlert>,
) -> serde_json::Value {
    serde_json::Value::Array(
        alerts
            .into_iter()
            .map(|alert| {
                serde_json::json!({
                    "labels": alert.labels,
                    "annotations": alert.annotations,
                    "startsAt": unix_ms_to_rfc3339(alert.starts_at_ms),
                    "endsAt": alert.ends_at_ms.map(unix_ms_to_rfc3339),
                    "generatorURL": alert.generator_url,
                })
            })
            .collect(),
    )
}

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

struct AlertBatch {
    tenant: Option<String>,
    alerts: Vec<krabka_promql::AlertmanagerAlert>,
}
type AlertSender = tokio::sync::mpsc::Sender<AlertBatch>;

/// A bounded FIFO in front of Alertmanager delivery.
///
/// Enqueueing is the evaluation-path success boundary. When the queue is full,
/// producers wait for capacity instead of dropping alerts; the worker retries
/// each batch until one configured endpoint accepts it.
#[derive(Clone)]
pub struct QueuedAlertmanagerSink {
    sender: std::sync::Arc<tokio::sync::Mutex<Option<AlertSender>>>,
    worker: std::sync::Arc<tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
    alert_templates: AlertTemplateDefaults,
}

impl QueuedAlertmanagerSink {
    #[must_use]
    pub fn new(
        sink: AlertmanagerHttpSink,
        capacity: usize,
        resend_delay: std::time::Duration,
    ) -> Self {
        let alert_templates = sink.alert_templates.clone();
        let channel = tokio::sync::mpsc::channel::<AlertBatch>(capacity.max(1));
        let sender = channel.0;
        let mut receiver = channel.1;
        let worker = tokio::spawn(async move {
            while let Some(batch) = receiver.recv().await {
                loop {
                    match sink
                        .deliver(batch.tenant.as_deref(), batch.alerts.clone())
                        .await
                    {
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
            alert_templates,
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

    async fn dispatch_batch(
        &self,
        tenant: Option<&str>,
        alerts: Vec<krabka_promql::AlertmanagerAlert>,
    ) -> Result<(), RulerWalError> {
        if alerts.is_empty() {
            return Ok(());
        }
        let sender = self.sender.lock().await.clone().ok_or_else(|| {
            RulerWalError::Append("alertmanager delivery queue stopped".to_string())
        })?;
        sender
            .send(AlertBatch {
                tenant: tenant.map(str::to_owned),
                alerts,
            })
            .await
            .map_err(|_| RulerWalError::Append("alertmanager delivery queue stopped".to_string()))
    }
}

impl_alertmanager_sink!(QueuedAlertmanagerSink);

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
    fn template_external_labels(&self) -> krabka_blockstore::Labels {
        match self {
            Self::Http(sink) => sink.template_external_labels(),
            Self::Noop(sink) => sink.template_external_labels(),
        }
    }

    fn template_external_url(&self, alert_name: &str) -> String {
        match self {
            Self::Http(sink) => sink.template_external_url(alert_name),
            Self::Noop(sink) => sink.template_external_url(alert_name),
        }
    }

    async fn dispatch_alerts(
        &self,
        alerts: Vec<krabka_promql::AlertmanagerAlert>,
    ) -> Result<(), RulerWalError> {
        match self {
            Self::Http(sink) => sink.dispatch_alerts(alerts).await,
            Self::Noop(sink) => sink.dispatch_alerts(alerts).await,
        }
    }

    async fn dispatch_alerts_for_tenant(
        &self,
        tenant: &krabka_blockstore::TenantId,
        alerts: Vec<krabka_promql::AlertmanagerAlert>,
    ) -> Result<(), RulerWalError> {
        match self {
            Self::Http(sink) => sink.dispatch_alerts_for_tenant(tenant, alerts).await,
            Self::Noop(sink) => sink.dispatch_alerts_for_tenant(tenant, alerts).await,
        }
    }
}
