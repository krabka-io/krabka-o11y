use super::{loki_decode_error_context, previous_char_boundary};

pub(crate) fn loki_json_push_payload_parse_error(body: &[u8]) -> String {
    let body = String::from_utf8_lossy(body);
    let value_start = body
        .char_indices()
        .find_map(|(index, char)| (!char.is_whitespace()).then_some(index))
        .unwrap_or(body.len());
    let found = body[value_start..].chars().next().unwrap_or('\0');
    let context_start = previous_char_boundary(&body, value_start);
    let context_end = previous_char_boundary(&body, body.len().min(context_start + 11));
    let context = &body[context_start..context_end];
    let bigger_context = loki_decode_error_context(&body, value_start);

    format!(
        "readObjectStart: expect {{ or n, but found {found}, error found in #1 byte of ...|{context}|..., bigger context ...|{bigger_context}|...\n"
    )
}
