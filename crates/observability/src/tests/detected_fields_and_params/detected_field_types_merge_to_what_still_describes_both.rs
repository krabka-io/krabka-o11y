use super::*;

/// Two sightings of the same field can disagree about its type, and the
/// merge picks what still describes both. The arms are ordered, so
/// deleting one does not fail -- it falls through to the catch-all and
/// quietly widens to a string. Only a pair that a *later* arm would also
/// match shows the difference, so the whole six-by-six table is here.
#[test]
pub(crate) fn detected_field_types_merge_to_what_still_describes_both() {
    use super::super::prelude::DetectedFieldType as Type;

    let cases = [
        (Type::Boolean, Type::Boolean, Type::Boolean),
        (Type::Boolean, Type::Int, Type::String),
        (Type::Boolean, Type::Float, Type::Float),
        (Type::Boolean, Type::Duration, Type::String),
        (Type::Boolean, Type::Bytes, Type::String),
        (Type::Boolean, Type::String, Type::String),
        (Type::Int, Type::Boolean, Type::String),
        (Type::Int, Type::Int, Type::Int),
        (Type::Int, Type::Float, Type::Float),
        (Type::Int, Type::Duration, Type::String),
        (Type::Int, Type::Bytes, Type::String),
        (Type::Int, Type::String, Type::String),
        (Type::Float, Type::Boolean, Type::Float),
        (Type::Float, Type::Int, Type::Float),
        (Type::Float, Type::Float, Type::Float),
        (Type::Float, Type::Duration, Type::Float),
        (Type::Float, Type::Bytes, Type::Float),
        (Type::Float, Type::String, Type::String),
        (Type::Duration, Type::Boolean, Type::String),
        (Type::Duration, Type::Int, Type::String),
        (Type::Duration, Type::Float, Type::Float),
        (Type::Duration, Type::Duration, Type::Duration),
        (Type::Duration, Type::Bytes, Type::String),
        (Type::Duration, Type::String, Type::String),
        (Type::Bytes, Type::Boolean, Type::String),
        (Type::Bytes, Type::Int, Type::String),
        (Type::Bytes, Type::Float, Type::Float),
        (Type::Bytes, Type::Duration, Type::String),
        (Type::Bytes, Type::Bytes, Type::Bytes),
        (Type::Bytes, Type::String, Type::String),
        (Type::String, Type::Boolean, Type::String),
        (Type::String, Type::Int, Type::String),
        (Type::String, Type::Float, Type::String),
        (Type::String, Type::Duration, Type::String),
        (Type::String, Type::Bytes, Type::String),
        (Type::String, Type::String, Type::String),
    ];

    for (left, right, want) in cases {
        check!(left.merge(right) == want, "{left:?} with {right:?}");
    }
}
