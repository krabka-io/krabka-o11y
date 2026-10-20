//! Record HTTP requests emitted by a real Grafana backend plugin.

use std::sync::{Arc, Mutex};

use axum::{
    Router,
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::oneshot};

type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

type Requests = Arc<Mutex<Vec<Value>>>;

#[derive(Clone)]
struct ProxyState {
    upstream: String,
    client: reqwest::Client,
    requests: Requests,
}

pub struct RecordingProxy {
    pub port: u16,
    requests: Requests,
    shutdown: Option<oneshot::Sender<()>>,
}

impl Drop for RecordingProxy {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

impl RecordingProxy {
    /// Start a recording proxy on a host-reachable ephemeral port.
    ///
    /// # Errors
    /// Returns an error if the listener cannot bind.
    pub async fn start(upstream: String) -> TestResult<Self> {
        let listener = TcpListener::bind("0.0.0.0:0").await?;
        let port = listener.local_addr()?.port();
        let requests = Requests::default();
        let state = ProxyState {
            upstream,
            client: reqwest::Client::new(),
            requests: requests.clone(),
        };
        let (shutdown, stop) = oneshot::channel();
        tokio::spawn(async move {
            let _ = axum::serve(listener, Router::new().fallback(forward).with_state(state))
                .with_graceful_shutdown(async move {
                    let _ = stop.await;
                })
                .await;
        });
        Ok(Self {
            port,
            requests,
            shutdown: Some(shutdown),
        })
    }

    /// Remove and return all requests recorded so far.
    ///
    /// # Errors
    /// Returns an error if a previous recorder task poisoned the lock.
    pub fn take(&self) -> TestResult<Vec<Value>> {
        Ok(std::mem::take(
            &mut *self
                .requests
                .lock()
                .map_err(|_| "request recorder poisoned")?,
        ))
    }
}

async fn forward(State(state): State<ProxyState>, request: Request<Body>) -> Response {
    match forward_request(state, request).await {
        Ok(response) => response,
        Err(error) => (StatusCode::BAD_GATEWAY, error).into_response(),
    }
}

async fn forward_request(state: ProxyState, request: Request<Body>) -> Result<Response, String> {
    let (parts, body) = request.into_parts();
    let body = axum::body::to_bytes(body, 1024 * 1024)
        .await
        .map_err(|error| error.to_string())?;
    let tenant = parts
        .headers
        .get("X-Scope-OrgID")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    state
        .requests
        .lock()
        .map_err(|_| "request recorder poisoned")?
        .push(json!({
            "method":parts.method.as_str(), "uri":parts.uri.to_string(), "tenant":tenant,
            "body":String::from_utf8_lossy(&body),
            "content_type":parts.headers.get("content-type").and_then(|value| value.to_str().ok()).unwrap_or_default(),
        }));
    let mut outgoing = state
        .client
        .request(parts.method, format!("{}{}", state.upstream, parts.uri))
        .header("X-Scope-OrgID", tenant)
        .body(body);
    if let Some(content_type) = parts.headers.get("content-type") {
        outgoing = outgoing.header("content-type", content_type);
    }
    let response = outgoing.send().await.map_err(|error| error.to_string())?;
    let mut result = Response::builder().status(response.status());
    if let Some(content_type) = response.headers().get("content-type") {
        result = result.header("content-type", content_type);
    }
    result
        .body(Body::from(
            response.bytes().await.map_err(|error| error.to_string())?,
        ))
        .map_err(|error| error.to_string())
}

#[tokio::test]
async fn records_and_forwards_form_posts_without_changing_tenant_or_body() -> TestResult<()> {
    use assert2::assert;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let server = tokio::spawn(
        axum::serve(
            listener,
            Router::new().fallback(|request: Request<Body>| async {
                let (parts, body) = request.into_parts();
                let bytes = axum::body::to_bytes(body, 1024)
                    .await
                    .expect("bounded form body");
                axum::Json(
                    json!({"method":parts.method.as_str(),"uri":parts.uri.to_string(),
            "tenant":parts.headers.get("X-Scope-OrgID").and_then(|value|value.to_str().ok()),
            "content_type":parts.headers.get("Content-Type").and_then(|value|value.to_str().ok()),
            "body":String::from_utf8_lossy(&bytes)}),
                )
            }),
        )
        .into_future(),
    );
    let proxy = RecordingProxy::start(format!("http://127.0.0.1:{port}")).await?;
    let response: Value = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{}/api/v1/query", proxy.port))
        .header("X-Scope-OrgID", "grafana")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body("query=sum%28foo%29&time=45")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let expected = json!({"method":"POST","uri":"/api/v1/query","tenant":"grafana",
        "content_type":"application/x-www-form-urlencoded","body":"query=sum%28foo%29&time=45"});
    assert!(response == expected);
    assert!(proxy.take()? == vec![expected]);
    assert!(proxy.take()?.is_empty());
    server.abort();
    Ok(())
}
