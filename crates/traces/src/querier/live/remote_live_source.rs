use super::*;

/// A live tier that a querier reads from a live-store over HTTP.
pub struct RemoteLiveSource {
    pub(crate) base_url: Url,
    pub(crate) trace_index: SharedTraceIndex,
    pub(crate) http: reqwest::Client,
}

impl RemoteLiveSource {
    /// A source that reads the live-store at `base_url` with `internal_client` applied.
    ///
    /// The client carries the token, the certificate and the CA bundle of the
    /// internal client, so a live-store with authentication or TLS on serves
    /// the querier as the internal principal. The querier has already checked
    /// the end user against the tenant.
    ///
    /// # Errors
    /// Returns the `reqwest` error when the client cannot be built, for
    /// example from an identity that the TLS backend refuses.
    pub fn new(
        base_url: Url,
        trace_index: SharedTraceIndex,
        internal_client: &InternalClient,
    ) -> std::result::Result<Self, reqwest::Error> {
        Ok(Self {
            base_url,
            trace_index,
            http: internal_client.apply(reqwest::Client::builder()).build()?,
        })
    }
}

#[async_trait::async_trait]
impl LiveSource for RemoteLiveSource {
    async fn span_batches(
        &self,
        tenant: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<RecordBatch>> {
        let mut url = self
            .base_url
            .join(LIVE_SPAN_BATCHES_PATH)
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        url.query_pairs_mut()
            .append_pair("start", &start_ns.to_string())
            .append_pair("end", &end_ns.to_string());
        let resp = self
            .http
            .get(url)
            .header(TENANT_HEADER, tenant)
            .send()
            .await
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        if !resp.status().is_success() {
            return Err(TraceqlError::Plan(format!(
                "remote live-store returned {}",
                resp.status()
            )));
        }
        let bytes = resp
            .bytes()
            .await
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        decode_span_batches(&bytes)
    }

    async fn trace_spans(&self, tenant: &str, trace_id: &[u8; 16]) -> Result<Option<TraceSpans>> {
        // Use the v1 endpoint for internal federation: it returns the bare OTLP
        // `TracesData` we decode below. The v2 endpoint wraps the trace in a
        // Tempo `TraceByIDResponse` for Grafana's backend datasource.
        let path = format!("/api/traces/{}", hex::encode(trace_id));
        let url = self
            .base_url
            .join(&path)
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        let resp = self
            .http
            .get(url)
            .header(TENANT_HEADER, tenant)
            .header("accept", "application/x-protobuf")
            .send()
            .await
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            return Err(TraceqlError::Plan(format!(
                "remote live-store returned {}",
                resp.status()
            )));
        }
        let bytes = resp
            .bytes()
            .await
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        let data = TracesData::decode(bytes).map_err(|err| TraceqlError::Plan(err.to_string()))?;
        trace_spans_from_otlp(trace_id, data).map(Some)
    }

    async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<ScopedTag>> {
        let mut url = self
            .base_url
            .join("/api/v2/search/tags")
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        {
            let mut query = url.query_pairs_mut();
            query
                .append_pair("start", &ns_floor_seconds(start_ns).to_string())
                .append_pair("end", &ns_ceil_seconds(end_ns).to_string());
            if let Some(scope) = scope {
                query.append_pair("scope", tag_scope_name(scope));
            }
        }
        let json = self.get_json(tenant, url).await?;
        scoped_tags_from_json(&json)
    }

    async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<TypedValue>> {
        let mut url = self
            .base_url
            .join(&format!("/api/v2/search/tag/{tag}/values"))
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        url.query_pairs_mut()
            .append_pair("start", &ns_floor_seconds(start_ns).to_string())
            .append_pair("end", &ns_ceil_seconds(end_ns).to_string());
        let json = self.get_json(tenant, url).await?;
        typed_values_from_json(&json)
    }

    fn block_builder_frontier_ns(&self, tenant: &str) -> i64 {
        let trace_index = self.trace_index.load();
        trace_index
            .trace_blocks(tenant)
            .iter()
            .map(|block| block.max_ts.saturating_add(1))
            .max()
            .unwrap_or_default()
    }
}

impl RemoteLiveSource {
    pub(crate) async fn get_json(&self, tenant: &str, url: Url) -> Result<serde_json::Value> {
        let resp = self
            .http
            .get(url)
            .header(TENANT_HEADER, tenant)
            .send()
            .await
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        if !resp.status().is_success() {
            return Err(TraceqlError::Plan(format!(
                "remote live-store returned {}",
                resp.status()
            )));
        }
        resp.json()
            .await
            .map_err(|err| TraceqlError::Plan(err.to_string()))
    }
}
