// What the suites that push spans through the distributor's doors share: an
// OTLP string attribute to put on the pushed spans, a push to one HTTP door, a
// WAL sink that keeps what the doors append, and a background server for the
// querier or distributor they then talk to.

use std::sync::{Arc, Mutex};

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt as _;
use krabka_traces::{SpanRecord, TracesError, distributor::WalSink};
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, KeyValue as OtlpKeyValue, any_value::Value,
};
use tower::ServiceExt as _;

/// A WAL sink that keeps every record appended to it, in order, instead of
/// sending it to Kafka.
#[derive(Clone, Default)]
pub struct CapturingSink {
    pub records: Arc<Mutex<Vec<SpanRecord>>>,
}

#[async_trait::async_trait]
impl WalSink for CapturingSink {
    async fn append(&self, rec: SpanRecord) -> Result<(), TracesError> {
        self.records
            .lock()
            .map_err(|_| TracesError::Wal("capturing sink lock poisoned".into()))?
            .push(rec);
        Ok(())
    }
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

impl CapturingSink {
    /// Every record appended so far, in order.
    pub fn snapshot(&self) -> Result<Vec<SpanRecord>, BoxError> {
        Ok(self
            .records
            .lock()
            .map_err(|_| "capturing sink lock poisoned")?
            .clone())
    }
}

/// One `POST` to an HTTP ingest door, and the status the door must answer.
pub struct DoorPush<'a> {
    pub uri: &'a str,
    pub content_type: &'a str,
    pub tenant: &'a str,
    pub body: Body,
    pub expected_status: StatusCode,
}

/// Sends `push` to `door`, asserts the door answers `push.expected_status`,
/// and drains the response.
pub async fn push_to_door(door: Router, push: DoorPush<'_>) -> Result<(), BoxError> {
    let resp = door
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(push.uri)
                .header("content-type", push.content_type)
                .header("x-scope-orgid", push.tenant)
                .body(push.body)?,
        )
        .await?;
    assert2::assert!(resp.status() == push.expected_status);
    let _ = resp.into_body().collect().await?;
    Ok(())
}

/// Serves `app` on `listener` in the background until the returned sender
/// fires or is dropped.
pub fn serve_until_shutdown(
    listener: tokio::net::TcpListener,
    app: Router,
) -> tokio::sync::oneshot::Sender<()> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = rx.await;
            })
            .await;
    });
    tx
}

/// An OTLP attribute with a string value.
pub fn string_kv(key: &str, value: &str) -> OtlpKeyValue {
    OtlpKeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.into())),
        }),
        ..OtlpKeyValue::default()
    }
}
