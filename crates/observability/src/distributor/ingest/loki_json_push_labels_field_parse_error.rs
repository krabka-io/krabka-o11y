use super::{loki_decode_error_context, loki_json_push_unclosed_object_error};

pub(crate) fn loki_json_push_labels_field_parse_error(body: &[u8]) -> String {
    let body = String::from_utf8_lossy(body);
    let context = loki_decode_error_context(&body, body.len().saturating_sub(12));
    let bigger_context = loki_decode_error_context(&body, body.len().saturating_sub(52));

    loki_json_push_unclosed_object_error(context, bigger_context)
}
