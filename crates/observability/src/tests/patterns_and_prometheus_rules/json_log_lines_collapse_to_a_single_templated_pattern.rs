use super::*;

#[test]
pub(crate) fn json_log_lines_collapse_to_a_single_templated_pattern() {
    // Two Krabka-shaped JSON log lines differing only by timestamp must mine
    // to one pattern with the timestamp templatized and every constant kept.
    let first = r#"{"timestamp":"2026-07-01T04:19:26.1238077Z","severity":"INFO","target":"krabka_broker::network::dispatch","message":"connection opened"}"#;
    let second = r#"{"timestamp":"2026-07-01T04:19:27.9981001Z","severity":"INFO","target":"krabka_broker::network::dispatch","message":"connection opened"}"#;
    assert_eq!(log_line_pattern(first), log_line_pattern(second));
    assert_eq!(
        log_line_pattern(first),
        r#"{"timestamp":"<_>","severity":"INFO","target":"krabka_broker::network::dispatch","message":"connection opened"}"#
    );
}
