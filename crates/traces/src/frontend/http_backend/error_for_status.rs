use super::BackendError;

pub(crate) async fn error_for_status(
    resp: reqwest::Response,
) -> Result<reqwest::Response, BackendError> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    let message = resp.text().await.unwrap_or_default();
    Err(BackendError::Backend {
        status: status.as_u16().to_string(),
        message,
    })
}
