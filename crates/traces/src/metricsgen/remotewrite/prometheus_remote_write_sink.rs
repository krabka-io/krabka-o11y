use super::*;

/// HTTP client for Prometheus `remote_write`.
pub struct PrometheusRemoteWriteSink {
    pub(crate) url: String,
    pub(crate) http: reqwest::Client,
}

impl PrometheusRemoteWriteSink {
    #[must_use]
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            http: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl RemoteWriteSink for PrometheusRemoteWriteSink {
    async fn write(&self, payload: &SeriesPayload) -> Result<(), SinkError> {
        let rows = to_timeseries(&payload.series);
        let body = encode_write_request(&rows).map_err(SinkError::Decode)?;
        let resp = self
            .http
            .post(&self.url)
            .header("Content-Type", "application/x-protobuf")
            .header("Content-Encoding", "snappy")
            .header("X-Prometheus-Remote-Write-Version", "0.1.0")
            .header("X-Scope-OrgID", &payload.tenant)
            .body(body)
            .send()
            .await
            .map_err(|err| SinkError::Transport(err.to_string()))?;

        if resp.status().is_success() {
            Ok(())
        } else {
            Err(SinkError::Transport(format!(
                "remote_write status {}",
                resp.status()
            )))
        }
    }
}
