use super::*;

pub(crate) fn loki_json_push_value_parse_error(body: &[u8], value: &Value) -> String {
    let body = String::from_utf8_lossy(body);
    let value_text = value.to_string();
    let value_start = body.find(&value_text).unwrap_or(body.len());
    let context = loki_decode_error_context(&body, value_start.saturating_add(10));
    let bigger_context = loki_decode_error_context(&body, value_start.saturating_sub(30));

    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Unknown value type, error found in #10 byte of ...|{context}|..., bigger context ...|{bigger_context}|...\n"
    )
}
