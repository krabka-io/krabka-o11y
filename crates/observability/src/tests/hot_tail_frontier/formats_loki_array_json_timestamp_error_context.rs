use super::*;

#[test]
pub(crate) fn formats_loki_array_json_timestamp_error_context() {
    let body = br#"{"streams":[{"stream":{"app":"api"},"values":[[["1000000000"],"array push timestamp"]]}]}"#;
    let timestamp = json!(["1000000000"]);
    let line = json!("array push timestamp");

    assert_eq!(
        loki_json_timestamp_value_parse_error(body, &timestamp, Some(&line)),
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like Number/Boolean/None, but can't find its end: ',' or '}' symbol, error found in #10 byte of ...|estamp\"]]}]}|..., bigger context ...|values\":[[[\"1000000000\"],\"array push timestamp\"]]}]}|...\n"
    );
}
