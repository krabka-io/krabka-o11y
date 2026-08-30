use super::*;

/// Generic message that clients get for any server-side (5xx) fault.
///
/// The server logs the detailed error with `tracing` and does not put it in the
/// response body. Internal details such as lock poisoning and WAL, produce, and
/// block internals never reach an untrusted caller.
pub(crate) const INTERNAL_ERROR_MESSAGE: &str = "internal server error";
