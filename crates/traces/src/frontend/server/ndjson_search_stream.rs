use std::convert::Infallible;

use futures::stream;
use tokio::sync::mpsc;

use super::{BackendError, Body, Bytes, IntoResponse, Response};
use crate::frontend::wire::SearchResponseJson;

/// The NDJSON body of `/api/search/stream`: one cumulative response per line,
/// or an `error` object for a shard that failed.
pub(crate) fn ndjson_search_stream(
    receiver: mpsc::Receiver<Result<SearchResponseJson, BackendError>>,
) -> Response {
    let body = stream::unfold(receiver, |mut receiver| async move {
        let item = receiver.recv().await?;
        let value = match item {
            Ok(response) => serde_json::to_vec(&response).unwrap_or_default(),
            Err(error) => serde_json::to_vec(&serde_json::json!({ "error": error.to_string() }))
                .unwrap_or_default(),
        };
        let mut line = value;
        line.push(b'\n');
        Some((Ok::<Bytes, Infallible>(Bytes::from(line)), receiver))
    });
    (
        [(("content-type"), "application/x-ndjson")],
        Body::from_stream(body),
    )
        .into_response()
}
