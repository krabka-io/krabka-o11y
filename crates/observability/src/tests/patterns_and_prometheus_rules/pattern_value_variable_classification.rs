use super::*;

#[test]
pub(crate) fn pattern_value_variable_classification() {
    // Variable: timestamps, floats, UUIDs, long hex ids, opaque tokens.
    assert!(pattern_value_is_variable("2026-07-01T04:19:26.1238077Z"));
    assert!(pattern_value_is_variable("42.5"));
    assert!(pattern_value_is_variable(
        "550e8400-e29b-41d4-a716-446655440000"
    ));
    assert!(pattern_value_is_variable(
        "4f3a9c2be18d4f6a5b7c9e0f1a2d3e4b"
    ));
    assert!(pattern_value_is_variable("AKIAIOSFODNN7EXAMPLE"));
    assert!(pattern_value_is_variable("\"2026-07-01T04:19:26Z\""));
    // Sole-reason coverage: each value below is variable via exactly one
    // classifier, so every branch of the `||` chain (and the shape checks
    // inside `is_uuid`/`is_hex_id`) is independently exercised.
    assert!(pattern_value_is_variable("-42.5")); // negative float: only the f64 parse
    assert!(pattern_value_is_variable(
        "f47ac10b-58cc-4372-a567-0e02b2c3d479" // letter-led UUID: only is_uuid
    ));
    assert!(pattern_value_is_variable("abcdefabcdefabcd")); // 16 hex letters, no digit: only is_hex_id
    // UUID *layout* but non-hex groups must not be accepted as a UUID (guards
    // the `len == n && all-hex` check inside is_uuid).
    assert!(!pattern_value_is_variable(
        "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    ));
    // Constant: levels, module paths, file:line callers, short words.
    assert!(!pattern_value_is_variable("INFO"));
    assert!(!pattern_value_is_variable(
        "krabka_broker::network::dispatch"
    ));
    assert!(!pattern_value_is_variable("grpc_logging.go:66"));
    assert!(!pattern_value_is_variable("/cortex.Ingester/Push"));
    assert!(!pattern_value_is_variable("cafe"));
    assert!(!pattern_value_is_variable("authenticationToken"));
}
