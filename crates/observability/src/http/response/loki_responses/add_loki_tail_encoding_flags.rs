use super::{Value, json};

/// Echoes the request's encoding flags into a tail frame.
///
/// A tail frame is not a query response, so the flags sit beside `streams`
/// rather than under a `data` object.
pub(crate) fn add_loki_tail_encoding_flags(frame: &mut Value, flags: &[String]) {
    if flags.is_empty() {
        return;
    }
    frame["encodingFlags"] = json!(flags);
}
