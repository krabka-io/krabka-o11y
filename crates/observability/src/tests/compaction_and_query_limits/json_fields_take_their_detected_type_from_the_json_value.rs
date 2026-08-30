use super::*;

/// A JSON field's detected type comes from the JSON type itself, not from
/// re-parsing its rendered text. Both integer widths count as `Int` --
/// serde reports a negative one as `i64` only and a very large one as
/// `u64` only, so either alone would demote the other to `Float`.
#[test]
pub(crate) fn json_fields_take_their_detected_type_from_the_json_value() {
    let line = r#"{"a_bool":true,"neg":-1,"huge":18446744073709551615,"real":1.5,"text":"hi","nothing":null}"#;
    let mut fields = BTreeMap::new();
    detect_json_fields(&mut fields, line);

    let stats = |ty, value: &str| DetectedFieldStats {
        ty,
        values: BTreeSet::from([value.to_owned()]),
        parsers: BTreeSet::from(["json"]),
    };
    let expected = BTreeMap::from([
        (
            "a_bool".to_owned(),
            stats(DetectedFieldType::Boolean, "true"),
        ),
        ("neg".to_owned(), stats(DetectedFieldType::Int, "-1")),
        (
            "huge".to_owned(),
            stats(DetectedFieldType::Int, "18446744073709551615"),
        ),
        ("real".to_owned(), stats(DetectedFieldType::Float, "1.5")),
        ("text".to_owned(), stats(DetectedFieldType::String, "hi")),
    ]);
    check!(fields == expected);
}
