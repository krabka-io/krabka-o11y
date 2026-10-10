use super::push_len;

/// Appends `text` to `out` as its length followed by its bytes.
pub(crate) fn push_string(out: &mut Vec<u8>, text: &str) {
    push_len(out, text.len());
    out.extend_from_slice(text.as_bytes());
}
