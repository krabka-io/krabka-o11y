use super::*;

/// `remove_empty_object_field` drops a field only when it is an object
/// with nothing in it. A field that holds something stays, and so does one
/// that is not an object at all -- an empty array is not an empty object.
#[test]
pub(crate) fn an_empty_object_field_is_removed_and_nothing_else_is() {
    let mut value = serde_json::json!({
        "empty": {},
        "full": {"a": 1},
        "array": [],
        "null": null,
    });
    for field in ["empty", "full", "array", "null"] {
        super::super::prelude::remove_empty_object_field(&mut value, field);
    }
    check!(
        value == serde_json::json!({"full": {"a": 1}, "array": [], "null": null}),
        "got {value}"
    );

    // A value that is not an object at all is left alone rather than
    // panicking on the way past.
    let mut scalar = serde_json::json!(7);
    super::super::prelude::remove_empty_object_field(&mut scalar, "empty");
    check!(scalar == serde_json::json!(7));
}
