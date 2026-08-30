pub(crate) fn loki_json_timestamp_parse_error(timestamp: &str, line: &str) -> String {
    let found_context = timestamp
        .char_indices()
        .nth(9)
        .map_or(timestamp, |(offset, _)| &timestamp[offset..]);
    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like Number/Boolean/None, but can't find its end: ',' or '}}' symbol, error found in #10 byte of ...|{found_context}\"]]}}]}}|..., bigger context ...|s\":[[\"{timestamp}\",\"{line}\"]]}}]}}|...\n"
    )
}
