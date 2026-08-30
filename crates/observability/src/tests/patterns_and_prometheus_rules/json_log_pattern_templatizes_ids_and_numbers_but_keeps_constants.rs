use super::*;

#[test]
pub(crate) fn json_log_pattern_templatizes_ids_and_numbers_but_keeps_constants() {
    let pattern = log_line_pattern(
        r#"{"severity":"INFO","request_id":"550e8400-e29b-41d4-a716-446655440000","trace":"4f3a9c2be18d4f6a5b7c9e0f1a2d3e4b","offset":12345,"sasl":false,"listener":"PLAIN"}"#,
    );
    assert_eq!(
        pattern,
        r#"{"severity":"INFO","request_id":"<_>","trace":"<_>","offset":"<_>","sasl":false,"listener":"PLAIN"}"#
    );
}
