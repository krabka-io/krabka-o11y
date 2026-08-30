use super::*;

pub(crate) fn loki_json_push_streams_parse_error(body: &[u8], value: &Value) -> String {
    let body = String::from_utf8_lossy(body);
    let value_text = value.to_string();
    let value_start = body.find(&value_text).unwrap_or(body.len());
    let context_start = previous_char_boundary(&body, value_start.saturating_sub(9));
    let context_end = previous_char_boundary(&body, body.len().min(context_start + 20));
    let context = &body[context_start..context_end];
    let bigger_context = loki_decode_error_context(&body, value_start.saturating_sub(11));

    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: decode slice: expect [ or n, but found \", error found in #10 byte of ...|{context}|..., bigger context ...|{bigger_context}|...\n"
    )
}
