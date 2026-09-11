use super::{
    BackendError, Duration, InternalClient, MetricsJobRequest, MetricsPartial, MetricsResponseJson,
    QuerierBackend, QuerierScheme, SearchJobRequest, SearchPartial, SearchResponseJson,
    TENANT_HEADER, TagNamesJobRequest, TagNamesPartial, TagValuesBody, TagValuesJobRequest,
    TagValuesPartial, TagsBody, TraceByIdJobRequest, TraceByIdResponseJson, TracePartial,
    async_trait, build_url, error_for_status, ns_to_seconds, push_shard_params, scope_param,
};

/// The HTTP transport to one querier at a time.
///
/// It holds no addresses of its own. Each request names the `host:port` the
/// frontend assigned it to, which is an address the membership saw ready
/// within the last refresh; the transport dials it, with the tenant in
/// `X-Scope-OrgID` and a per-request timeout.
///
/// It used to hold a `Vec<String>` and an atomic counter, and pick a target
/// with `next.fetch_add(1) % addrs.len()`. That put a fixed 1/N of every
/// query on an address that might be gone and offered no way to use one that
/// had appeared.
pub struct HttpQuerier {
    pub(crate) http: reqwest::Client,
    pub(crate) scheme: QuerierScheme,
}

impl HttpQuerier {
    /// Build the transport. `timeout` bounds one job.
    ///
    /// Every job dials `scheme://host:port`. The client carries
    /// `internal_client`: its token as the `Authorization` header of every
    /// job, its certificate as the TLS identity, and its CA bundle as the only
    /// roots that a querier certificate can chain to. A querier with
    /// authentication on then serves the frontend as the internal principal.
    /// The frontend has already checked the end user against the tenant, so
    /// that principal should hold every tenant the frontend serves.
    ///
    /// # Errors
    /// Returns `BackendError::Transport` if the client cannot be built.
    pub fn new(
        timeout: Duration,
        scheme: QuerierScheme,
        internal_client: &InternalClient,
    ) -> Result<Self, BackendError> {
        let http = internal_client
            .apply(reqwest::Client::builder().timeout(timeout))
            .build()
            .map_err(|e| BackendError::Transport(e.to_string()))?;
        Ok(Self { http, scheme })
    }

    pub(crate) fn map_send_err(e: &reqwest::Error) -> BackendError {
        if e.is_timeout() {
            BackendError::Timeout
        } else {
            BackendError::Transport(e.to_string())
        }
    }
}

#[async_trait]
impl QuerierBackend for HttpQuerier {
    async fn search_job(&self, req: &SearchJobRequest) -> Result<SearchPartial, BackendError> {
        let url = format!("{}://{}/api/search", self.scheme.as_str(), req.querier);
        let mut params: Vec<(&str, String)> = vec![
            ("q", req.query.clone()),
            ("start", ns_to_seconds(req.start_ns)),
            ("end", ns_to_seconds(req.end_ns)),
            ("limit", req.limit.to_string()),
            ("spss", req.spss.to_string()),
        ];
        push_shard_params(&mut params, &req.shard);
        let resp = self
            .http
            .get(build_url(&url, &params)?)
            .header(TENANT_HEADER, req.tenant.as_str())
            .send()
            .await
            .map_err(|e| Self::map_send_err(&e))?;
        let resp = error_for_status(resp).await?;
        let body: SearchResponseJson = resp
            .json()
            .await
            .map_err(|e| BackendError::Transport(format!("decode search body: {e}")))?;
        Ok(SearchPartial {
            traces: body.traces,
            metrics: body.metrics,
        })
    }

    async fn trace_by_id_job(
        &self,
        req: &TraceByIdJobRequest,
    ) -> Result<TracePartial, BackendError> {
        let hex = crate::frontend::wire::hex16(&req.trace_id);
        let url = format!(
            "{}://{}/api/v2/traces/{hex}",
            self.scheme.as_str(),
            req.querier
        );
        let params: Vec<(&str, String)> = vec![
            ("start", ns_to_seconds(req.start_ns)),
            ("end", ns_to_seconds(req.end_ns)),
        ];
        let resp = self
            .http
            .get(build_url(&url, &params)?)
            .header(TENANT_HEADER, req.tenant.as_str())
            .send()
            .await
            .map_err(|e| Self::map_send_err(&e))?;
        // The querier returns 404 when it does not hold the trace; treat that as
        // an empty partial rather than an error (another querier may have it).
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(TracePartial::default());
        }
        let resp = error_for_status(resp).await?;
        let body: TraceByIdResponseJson = resp
            .json()
            .await
            .map_err(|e| BackendError::Transport(format!("decode trace body: {e}")))?;
        Ok(TracePartial {
            trace: body,
            metrics: crate::frontend::wire::Metrics::default(),
        })
    }

    async fn tag_names_job(
        &self,
        req: &TagNamesJobRequest,
    ) -> Result<TagNamesPartial, BackendError> {
        let url = format!(
            "{}://{}/api/v2/search/tags",
            self.scheme.as_str(),
            req.querier
        );
        let mut params: Vec<(&str, String)> = vec![
            ("start", ns_to_seconds(req.start_ns)),
            ("end", ns_to_seconds(req.end_ns)),
        ];
        if let Some(scope) = req.scope {
            params.push(("scope", scope_param(scope).to_string()));
        }
        push_shard_params(&mut params, &req.shard);
        let resp = self
            .http
            .get(build_url(&url, &params)?)
            .header(TENANT_HEADER, req.tenant.as_str())
            .send()
            .await
            .map_err(|e| Self::map_send_err(&e))?;
        let resp = error_for_status(resp).await?;
        let body: TagsBody = resp
            .json()
            .await
            .map_err(|e| BackendError::Transport(format!("decode tags body: {e}")))?;
        Ok(TagNamesPartial {
            tags: body.scoped_tags(),
            metrics: body.metrics,
        })
    }

    async fn tag_values_job(
        &self,
        req: &TagValuesJobRequest,
    ) -> Result<TagValuesPartial, BackendError> {
        // The tag is a client-supplied path segment (e.g. `span:name`,
        // `resource.service.name`); build it via `path_segments_mut` so any
        // special chars (`/`, `?`, `#`, space) are percent-encoded into a single
        // segment rather than corrupting the path/query when re-parsed.
        let mut url = reqwest::Url::parse(&format!("{}://{}", self.scheme.as_str(), req.querier))
            .map_err(|e| BackendError::Transport(format!("invalid querier addr: {e}")))?;
        url.path_segments_mut()
            .map_err(|()| BackendError::Transport("querier url cannot be a base".to_string()))?
            .extend(["api", "v2", "search", "tag", req.tag.as_str(), "values"]);
        let mut params: Vec<(&str, String)> = vec![
            ("start", ns_to_seconds(req.start_ns)),
            ("end", ns_to_seconds(req.end_ns)),
        ];
        push_shard_params(&mut params, &req.shard);
        {
            let mut pairs = url.query_pairs_mut();
            for (key, value) in &params {
                pairs.append_pair(key, value);
            }
        }
        let resp = self
            .http
            .get(url)
            .header(TENANT_HEADER, req.tenant.as_str())
            .send()
            .await
            .map_err(|e| Self::map_send_err(&e))?;
        let resp = error_for_status(resp).await?;
        let body: TagValuesBody = resp
            .json()
            .await
            .map_err(|e| BackendError::Transport(format!("decode tag-values body: {e}")))?;
        let metrics = body.metrics;
        Ok(TagValuesPartial {
            values: body.into_typed_values(),
            metrics,
        })
    }

    async fn metrics_job(&self, req: &MetricsJobRequest) -> Result<MetricsPartial, BackendError> {
        let path = if req.instant { "query" } else { "query_range" };
        let url = format!(
            "{}://{}/api/metrics/{path}",
            self.scheme.as_str(),
            req.querier
        );
        let mut params: Vec<(&str, String)> = vec![
            ("q", req.query.clone()),
            ("start", ns_to_seconds(req.start_ns)),
            ("end", ns_to_seconds(req.end_ns)),
        ];
        if !req.instant {
            params.push(("step", ns_to_seconds(req.step_ns)));
        }
        push_shard_params(&mut params, &req.shard);
        let resp = self
            .http
            .get(build_url(&url, &params)?)
            .header(TENANT_HEADER, req.tenant.as_str())
            .send()
            .await
            .map_err(|e| Self::map_send_err(&e))?;
        let resp = error_for_status(resp).await?;
        let response: MetricsResponseJson = resp
            .json()
            .await
            .map_err(|e| BackendError::Transport(format!("decode metrics body: {e}")))?;
        Ok(MetricsPartial {
            response,
            metrics: crate::frontend::wire::Metrics::default(),
        })
    }
}
