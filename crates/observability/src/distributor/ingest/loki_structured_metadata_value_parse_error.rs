use super::*;

pub(crate) fn loki_structured_metadata_value_parse_error(
    body: &[u8],
    name: &str,
    value: &Value,
) -> String {
    let body = String::from_utf8_lossy(body);
    let key = quote_logql_string(name);
    let needle = format!("{key}:{value}");
    let value_start = body.find(&needle).map_or_else(
        || body.find(&value.to_string()).unwrap_or(body.len()),
        |offset| offset + key.len() + 1,
    );
    let context = loki_decode_error_context(&body, value_start.saturating_sub(3));
    let bigger_context = loki_decode_error_context(&body, value_start.saturating_sub(43));

    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value is string, but can't find closing '\"' symbol, error found in #10 byte of ...|{context}|..., bigger context ...|{bigger_context}|...\n"
    )
}
