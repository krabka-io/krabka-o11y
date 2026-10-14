use std::{
    convert::Infallible,
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;
use tokio::sync::mpsc;

use super::{BackendError, Body, Bytes, Response};
use crate::frontend::wire::SearchResponseJson;

/// The NDJSON body of `/api/search/stream`: one cumulative response per line,
/// or an `error` object for a shard that failed.
pub(crate) fn ndjson_search_stream(
    receiver: mpsc::Receiver<Result<SearchResponseJson, BackendError>>,
) -> Response {
    let body = NdjsonSearchStream { receiver };
    Response::builder()
        .header("content-type", "application/x-ndjson")
        .body(Body::from_stream(body))
        .expect("the HTTP response builds")
}

struct NdjsonSearchStream {
    receiver: mpsc::Receiver<Result<SearchResponseJson, BackendError>>,
}

impl Stream for NdjsonSearchStream {
    type Item = Result<Bytes, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let item = match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(item)) => item,
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => return Poll::Pending,
        };
        let value = match item {
            Ok(response) => serde_json::to_vec(&response).unwrap_or_default(),
            Err(error) => serde_json::to_vec(&serde_json::json!({ "error": error.to_string() }))
                .unwrap_or_default(),
        };
        let mut line = value;
        line.push(b'\n');
        Poll::Ready(Some(Ok(Bytes::from(line))))
    }
}
