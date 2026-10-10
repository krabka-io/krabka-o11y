use serde::de::DeserializeOwned;

use super::{BackendError, error_for_status};

/// Refuses a non-success querier response, then decodes its JSON body.
///
/// `body_kind` names the body in the transport error, as in
/// `decode <body_kind> body: <cause>`.
pub(crate) async fn decode_json_body<T: DeserializeOwned>(
    resp: reqwest::Response,
    body_kind: &str,
) -> Result<T, BackendError> {
    error_for_status(resp)
        .await?
        .json()
        .await
        .map_err(|e| BackendError::Transport(format!("decode {body_kind} body: {e}")))
}
