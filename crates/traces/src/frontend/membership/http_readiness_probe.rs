use super::{BackendError, Duration, QuerierHealth, ReadinessProbe, async_trait};

/// Reads a querier's `/ready`, the endpoint every Krabka role already serves.
///
/// `krabka_observability::RoleReadiness` answers 200 when every registered
/// gate is met, and 503 with `not ready: <gate>, <gate>` when some are not.
/// This probe keeps that distinction: a 503 is a querier that will come back,
/// and its gate names go into the response warning verbatim.
pub struct HttpReadinessProbe {
    http: reqwest::Client,
}

impl HttpReadinessProbe {
    /// Build the probe client. `timeout` bounds a single probe, and wants to
    /// be far shorter than a query timeout: a probe that hangs holds up the
    /// whole refresh.
    ///
    /// # Errors
    /// Returns `BackendError::Transport` when the HTTP client cannot be built.
    pub fn new(timeout: Duration) -> Result<Self, BackendError> {
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| BackendError::Transport(e.to_string()))?;
        Ok(Self { http })
    }
}

#[async_trait]
impl ReadinessProbe for HttpReadinessProbe {
    async fn probe(&self, addr: &str) -> QuerierHealth {
        let resp = match self.http.get(format!("http://{addr}/ready")).send().await {
            Ok(resp) => resp,
            Err(error) => {
                return QuerierHealth::Unreachable {
                    error: error.to_string(),
                };
            }
        };
        let status = resp.status();
        if status.is_success() {
            return QuerierHealth::Ready;
        }
        if status != reqwest::StatusCode::SERVICE_UNAVAILABLE {
            return QuerierHealth::Unreachable {
                error: format!("/ready returned {status}"),
            };
        }
        let body = resp.text().await.unwrap_or_default();
        QuerierHealth::NotReady {
            pending: pending_gates(&body),
        }
    }
}

/// Pull the gate names out of a `RoleReadiness` 503 body.
///
/// The body is `not ready: <names>\n`. Anything else -- another component's
/// 503, an empty body -- still means unready, so it reports an unnamed gate
/// rather than claiming to know which one.
pub(super) fn pending_gates(body: &str) -> String {
    let named = body
        .trim()
        .strip_prefix("not ready:")
        .map(str::trim)
        .unwrap_or_default();
    if named.is_empty() {
        "unnamed gate".to_string()
    } else {
        named.to_string()
    }
}
