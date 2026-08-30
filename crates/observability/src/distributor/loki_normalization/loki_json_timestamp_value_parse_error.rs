use super::*;

pub(crate) fn loki_json_timestamp_value_parse_error(
    body: &[u8],
    timestamp: &Value,
    line: Option<&Value>,
) -> String {
    let body = String::from_utf8_lossy(body);
    let timestamp_text = timestamp.to_string();
    let value_start = body.find(&timestamp_text).unwrap_or(body.len());
    let found_context = line.and_then(Value::as_str).map_or_else(
        || loki_decode_error_context(&body, value_start.saturating_add(10)).to_string(),
        |line| {
            let start = line
                .char_indices()
                .nth(line.chars().count().saturating_sub(6))
                .map_or(0, |(offset, _)| offset);
            format!("{}\"]]}}]}}", &line[start..])
        },
    );
    let context_prefix_len = if timestamp.is_array() {
        10
    } else if timestamp.is_object() {
        4
    } else {
        9
    };
    let bigger_context =
        loki_decode_error_context(&body, value_start.saturating_sub(context_prefix_len));

    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like Number/Boolean/None, but can't find its end: ',' or '}}' symbol, error found in #10 byte of ...|{found_context}|..., bigger context ...|{bigger_context}|...\n"
    )
}
