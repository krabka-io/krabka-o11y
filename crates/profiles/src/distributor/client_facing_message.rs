use super::{INTERNAL_ERROR_MESSAGE, ProfilesError};

/// Returns the client-facing message for `err`.
///
/// A client-input fault is a 4xx: a bad format, a decode or gunzip failure, an
/// invalid request, or an oversized payload. Its specific message is safe and
/// useful, so this function returns it verbatim. For any 5xx internal fault the
/// function logs the detailed error on the server and returns a generic
/// message. The call sites handle `LimitError` with their own projection.
pub(crate) fn client_facing_message(err: &ProfilesError) -> String {
    if err.status_code() >= 500 {
        tracing::error!(error = %err, "profiles distributor internal error");
        INTERNAL_ERROR_MESSAGE.to_string()
    } else {
        err.to_string()
    }
}
