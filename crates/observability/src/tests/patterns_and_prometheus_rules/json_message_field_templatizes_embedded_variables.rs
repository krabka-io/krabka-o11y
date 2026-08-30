use super::*;

#[test]
pub(crate) fn json_message_field_templatizes_embedded_variables() {
    assert_eq!(
        log_line_pattern(r#"{"message":"processed request 550e8400e29b41d4a716 in 42ms"}"#),
        r#"{"message":"processed request <_> in <_>"}"#
    );
}
