use super::{RemoteReadError, v1};

/// Chooses the first response type in the client's FIFO preference list that
/// this server implements. An absent list means the legacy `SAMPLES` format.
///
/// # Errors
///
/// Returns [`RemoteReadError::UnsupportedResponseTypes`] when the list contains
/// no supported response type.
pub fn negotiate_read_response_type(accepted: &[i32]) -> Result<v1::ResponseType, RemoteReadError> {
    if accepted.is_empty() {
        return Ok(v1::ResponseType::Samples);
    }
    accepted
        .iter()
        .find_map(|response_type| match *response_type {
            value if value == v1::ResponseType::Samples as i32 => Some(v1::ResponseType::Samples),
            value if value == v1::ResponseType::StreamedXorChunks as i32 => {
                Some(v1::ResponseType::StreamedXorChunks)
            }
            _ => None,
        })
        .ok_or_else(|| RemoteReadError::UnsupportedResponseTypes(accepted.to_vec()))
}
