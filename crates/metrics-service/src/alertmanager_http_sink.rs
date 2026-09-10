use super::*;

pub struct AlertmanagerHttpSink {
    pub(crate) client: reqwest::Client,
    pub(crate) endpoints: Vec<String>,
    pub(crate) external_labels: BTreeMap<String, String>,
    pub(crate) generator_url_template: Option<String>,
    pub(crate) max_attempts: usize,
    pub(crate) retry_delay: Duration,
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
        )
    }

    #[must_use]
    pub fn with_delivery(
        endpoints: Vec<String>,
        external_labels: BTreeMap<String, String>,
        generator_url_template: Option<String>,
        max_attempts: usize,
        retry_delay: Duration,
    ) -> Self {
        Self {
            client: reqwest::Client::new(),
            endpoints,
            external_labels,
            generator_url_template,
            max_attempts: max_attempts.max(1),
            retry_delay,
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
                alert.generator_url = template.replace("{alertname}", alert_name);
            }
        }
    }
}

#[async_trait::async_trait]
impl AlertmanagerSink for AlertmanagerHttpSink {
    async fn dispatch_alerts(
        &self,
        mut alerts: Vec<krabka_promql::AlertmanagerAlert>,
    ) -> Result<(), RulerWalError> {
        if alerts.is_empty() {
            return Ok(());
        }
        self.enrich(&mut alerts);
        let payload = alertmanager_payload(alerts);
        let mut failures = Vec::new();
        for endpoint in &self.endpoints {
            for attempt in 1..=self.max_attempts {
                match self.client.post(endpoint).json(&payload).send().await {
                    Ok(response) if response.status().is_success() => return Ok(()),
                    Ok(response) => {
                        failures.push(format!("{endpoint}: HTTP {}", response.status()))
                    }
                    Err(error) => failures.push(format!("{endpoint}: {error}")),
                }
                if attempt < self.max_attempts {
                    tokio::time::sleep(self.retry_delay).await;
                }
            }
        }
        Err(RulerWalError::Append(format!(
            "all alertmanager endpoints failed after bounded retries: {}",
            failures.join("; ")
        )))
    }
}
