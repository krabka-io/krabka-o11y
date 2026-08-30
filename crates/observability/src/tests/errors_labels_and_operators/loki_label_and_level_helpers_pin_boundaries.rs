use super::*;

#[test]
pub(crate) fn loki_label_and_level_helpers_pin_boundaries() {
    let rendered_labels = BTreeMap::from([
        ("app".to_string(), "api".to_string()),
        ("env".to_string(), "prod".to_string()),
    ]);
    assert_eq!(
        loki_label_set(&rendered_labels),
        r#"{app="api",env="prod"}"#
    );
    check!(loki_push_label_parse_error(&rendered_labels, "bad-name").contains("1:5"));
    // Every character of "bad-name" is judged the same way whether or not
    // it is treated as the first, so that case cannot tell the two apart.
    // A digit can: it is allowed anywhere except at the start.
    let digit_then_invalid = loki_push_label_parse_error(&rendered_labels, "b9-name");
    check!(
        digit_then_invalid.contains("1:4"),
        "the hyphen is the third character: {digit_then_invalid}"
    );
    check!(
        digit_then_invalid.contains("'-'"),
        "and the hyphen is what is reported: {digit_then_invalid}"
    );
    check!(
        loki_proto_label_parse_error(r#"{9bad="x"}"#)
            .unwrap()
            .contains("1:2")
    );
    check!(
        loki_proto_label_parse_error(r#"{app="api",9bad="x"}"#)
            .unwrap()
            .contains("1:12")
    );
    // A digit is fine once a name has started. Both cases above are
    // rejections, so without this the tracking could judge every character
    // by the first one's rule and they would still pass.
    check!(loki_proto_label_parse_error(r#"{a9="x"}"#).is_none());
    check!(loki_proto_label_parse_error(r#"{app="api",b9="x"}"#).is_none());

    // A comma starts a new name even when no `=` came between: in
    // `{app="api",...}` the `=` has already reset the tracking, so only a
    // list without values shows the comma doing it.
    check!(
        loki_proto_label_parse_error("{app,9bad}")
            .unwrap()
            .contains("1:6")
    );
    check!(loki_proto_label_parse_error("{app,b9}").is_none());
    // After `=` the parser stops looking for a name, so an unquoted value
    // is not judged as one. A quoted value never shows this: the string
    // handling swallows it before the name check is reached.
    check!(loki_proto_label_parse_error("{app=bad-value}").is_none());

    let mut detected = BTreeMap::from([("app".to_string(), "api".to_string())]);
    discover_detected_level_label(&mut detected, "api ERROR happened");
    assert_eq!(
        detected.get("detected_level").map(String::as_str),
        Some("error")
    );
    // Any one of the four labels already present stops the discovery, and
    // each has to be the ONLY one present -- a guard that needed two of
    // them would still be stopped by a pair.
    for held in ["detected_level", "level", "severity", "severity_text"] {
        let mut labels = BTreeMap::from([(held.to_string(), "custom".to_string())]);
        discover_detected_level_label(&mut labels, "api error happened");
        check!(
            labels.get("detected_level").map(String::as_str)
                == if held == "detected_level" {
                    Some("custom")
                } else {
                    None
                },
            "{held} alone stops the discovery"
        );
    }
    for (line, want) in [
        ("error happened", true),
        ("happened error", true),
        ("terror", false),
        ("error_code", false),
    ] {
        assert_eq!(contains_log_level_token(line, "error"), want);
    }
    for (byte, want) in [(b'a', true), (b'1', true), (b'_', true), (b'-', false)] {
        assert_eq!(is_log_level_word_byte(byte), want);
    }
}
