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

pub struct AlertmanagerHttpSink {
    pub(crate) client: reqwest::Client,
    pub(crate) endpoints: Vec<String>,
    pub(crate) external_labels: BTreeMap<String, String>,
    pub(crate) generator_url_template: Option<String>,
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
            external_labels,
            generator_url_template,
            max_attempts: max_attempts.max(1),
            retry_delay,
            request_timeout,
        }
    }

    fn enrich(&self, alerts: &mut [krabka_promql::AlertmanagerAlert]) {
        for alert in alerts {
            for (name, value) in &self.external_labels {
                alert
                    .labels
                    .entry(name.clone())
                    .or_insert_with(|| value.clone());
            }
            if alert.generator_url.is_empty()
                && let Some(template) = &self.generator_url_template
            {
                let alert_name = alert.labels.get("alertname").map_or("", String::as_str);
                let alert_name = encode_url_component(alert_name);
                alert.generator_url = template.replace("{alertname}", &alert_name);
            }
        }
    }

    pub(crate) async fn deliver(
        &self,
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
                match self
                    .client
                    .post(endpoint)
                    .timeout(self.request_timeout)
                    .json(&payload)
                    .send()
                    .await
                {
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

#[async_trait::async_trait]
impl AlertmanagerSink for AlertmanagerHttpSink {
    fn template_external_labels(&self) -> krabka_blockstore::Labels {
        krabka_blockstore::Labels::from_pairs(self.external_labels.clone())
    }

    fn template_external_url(&self, alert_name: &str) -> String {
        self.generator_url_template
            .as_ref()
            .map_or_else(String::new, |template| {
                template.replace("{alertname}", &encode_url_component(alert_name))
            })
    }

    async fn dispatch_alerts(
        &self,
        alerts: Vec<krabka_promql::AlertmanagerAlert>,
    ) -> Result<(), RulerWalError> {
        self.deliver(alerts)
            .await
            .map_err(|error| RulerWalError::Append(error.to_string()))
    }
}
