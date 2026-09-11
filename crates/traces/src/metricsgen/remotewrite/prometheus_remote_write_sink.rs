use super::{
    InternalClient, RemoteWriteSink, SeriesPayload, SinkError, TENANT_HEADER, async_trait,
    encode_write_request, to_timeseries,
};

/// HTTP client for Prometheus `remote_write`.
pub struct PrometheusRemoteWriteSink {
    pub(crate) url: String,
    pub(crate) http: reqwest::Client,
}

impl PrometheusRemoteWriteSink {
    /// A sink that writes to `url` with `internal_client` applied.
    ///
    /// The target is a Krabka metrics distributor, so the sink presents the
    /// internal client: its token as the `Authorization` header, its
    /// certificate as the TLS identity, and its CA bundle as the only roots
    /// that the distributor certificate can chain to. With no
    /// `--internal-client-*` flag, the client is a plain `reqwest` client.
    ///
    /// # Errors
    /// Returns the `reqwest` error when the client cannot be built, for
    /// example from an identity that the TLS backend refuses.
    pub fn new(
        url: impl Into<String>,
        internal_client: &InternalClient,
    ) -> Result<Self, reqwest::Error> {
        Ok(Self {
            url: url.into(),
            http: internal_client.apply(reqwest::Client::builder()).build()?,
        })
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
            .header(TENANT_HEADER, &payload.tenant)
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
