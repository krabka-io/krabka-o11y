use super::*;

pub(crate) fn loki_decode_error_context(body: &str, start: usize) -> &str {
    let start = previous_char_boundary(body, start.min(body.len()));
    let end = previous_char_boundary(body, body.len().min(start + 80));
    &body[start..end]
}
