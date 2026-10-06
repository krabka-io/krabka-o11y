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

    let stream = BTreeMap::from([("app".to_string(), "api".to_string())]);
    let mut detected = BTreeMap::new();
    discover_detected_level_label(
        &stream,
        &mut detected,
        "api ERROR happened",
        &Limits::default(),
    );
    check!(detected.get("detected_level").map(String::as_str) == Some("error"));
    check!(stream == BTreeMap::from([("app".to_string(), "api".to_string())]));
    for held in ["level", "severity", "severity_text"] {
        let labels = BTreeMap::from([(held.to_string(), "custom".to_string())]);
        let mut metadata = BTreeMap::new();
        discover_detected_level_label(
            &labels,
            &mut metadata,
            "api error happened",
            &Limits::default(),
        );
        check!(metadata.get("detected_level").map(String::as_str) == Some("custom"));
        check!(labels.len() == 1);
    }
}
