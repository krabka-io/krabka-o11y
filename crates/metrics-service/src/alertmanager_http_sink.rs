use std::{collections::BTreeMap, time::Duration};

use super::{AlertmanagerSink, RulerWalError, alertmanager_payload};

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
        krabka_blockstore::Labels::from_pairs(self.external_labels.clone())
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
            for (name, value) in &self.alert_templates.external_labels {
                alert
                    .labels
                    .entry(name.clone())
                    .or_insert_with(|| value.clone());
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
        for (endpoint_index, endpoint) in self.endpoints.iter().enumerate() {
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
pub(crate) use impl_alertmanager_sink;

impl_alertmanager_sink!(AlertmanagerHttpSink);
