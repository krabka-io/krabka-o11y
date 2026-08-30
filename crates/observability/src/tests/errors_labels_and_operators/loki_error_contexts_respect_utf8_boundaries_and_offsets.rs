use super::*;

#[test]
pub(crate) fn loki_error_contexts_respect_utf8_boundaries_and_offsets() {
    let body = "{\"streams\":\"not-array\"}";
    assert!(
        loki_json_push_streams_parse_error(body.as_bytes(), &json!("not-array"))
            .contains("|{\"streams\":\"not-array\"}|")
    );
    // That assertion reads the *bigger* context, which is the whole body
    // either way. The narrow window is twenty bytes from nine before the
    // value, and stops short of the closing brace.
    check!(
        loki_json_push_streams_parse_error(body.as_bytes(), &json!("not-array"))
            .contains(r#"...|streams":"not-array"|..."#),
        "the narrow window is twenty bytes wide"
    );

    // The payload error's window is eleven bytes from the first
    // non-whitespace byte. A body that starts with one puts that at zero,
    // which is the only offset where a width computed by multiplying
    // rather than adding gives a different answer.
    check!(
        loki_json_push_payload_parse_error(b"\"not-json-at-all\"")
            .contains(r#"...|"not-json-a|..."#),
        "eleven bytes from the start"
    );

    let structured =
        br#"{"streams":[{"stream":{"app":"api"},"values":[["1","line",{"ok":true}]]}]}"#;
    let error = loki_structured_metadata_value_parse_error(structured, "ok", &json!(true));
    // The context window starts three bytes before the VALUE, which sits
    // one past the quoted key and its colon. A `contains` on the key and
    // value together is satisfied by any nearby offset -- the whole
    // eighty-byte window holds them -- so the window is pinned exactly.
    check!(
        error.contains(r#"...|k":true}]]}]}|..."#),
        "the context starts three bytes into the value: {error}"
    );

    let text = "ab\u{20ac}cd";
    assert_eq!(previous_char_boundary(text, 4), 2);
    assert_eq!(previous_char_boundary(text, text.len()), text.len());
}
