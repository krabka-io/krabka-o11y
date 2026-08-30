use super::loki_decode_error_context;

pub(crate) fn loki_json_push_labels_field_parse_error(body: &[u8]) -> String {
    let body = String::from_utf8_lossy(body);
    let context = loki_decode_error_context(&body, body.len().saturating_sub(12));
    let bigger_context = loki_decode_error_context(&body, body.len().saturating_sub(52));

    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like object, but can't find closing '}}' symbol, error found in #10 byte of ...|{context}|..., bigger context ...|{bigger_context}|...\n"
    )
}
