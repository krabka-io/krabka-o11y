use super::{super::prelude::DetectedFieldType as Type, *};

/// Two sightings of the same field can disagree about its type, and the
/// merge picks what still describes both.
#[yare::parameterized(
    boolean_boolean = { Type::Boolean, Type::Boolean, Type::Boolean },
    boolean_int = { Type::Boolean, Type::Int, Type::String },
    boolean_float = { Type::Boolean, Type::Float, Type::Float },
    boolean_duration = { Type::Boolean, Type::Duration, Type::String },
    boolean_bytes = { Type::Boolean, Type::Bytes, Type::String },
    boolean_string = { Type::Boolean, Type::String, Type::String },
    int_boolean = { Type::Int, Type::Boolean, Type::String },
    int_int = { Type::Int, Type::Int, Type::Int },
    int_float = { Type::Int, Type::Float, Type::Float },
    int_duration = { Type::Int, Type::Duration, Type::String },
    int_bytes = { Type::Int, Type::Bytes, Type::String },
    int_string = { Type::Int, Type::String, Type::String },
    float_boolean = { Type::Float, Type::Boolean, Type::Float },
    float_int = { Type::Float, Type::Int, Type::Float },
    float_float = { Type::Float, Type::Float, Type::Float },
    float_duration = { Type::Float, Type::Duration, Type::Float },
    float_bytes = { Type::Float, Type::Bytes, Type::Float },
    float_string = { Type::Float, Type::String, Type::String },
    duration_boolean = { Type::Duration, Type::Boolean, Type::String },
    duration_int = { Type::Duration, Type::Int, Type::String },
    duration_float = { Type::Duration, Type::Float, Type::Float },
    duration_duration = { Type::Duration, Type::Duration, Type::Duration },
    duration_bytes = { Type::Duration, Type::Bytes, Type::String },
    duration_string = { Type::Duration, Type::String, Type::String },
    bytes_boolean = { Type::Bytes, Type::Boolean, Type::String },
    bytes_int = { Type::Bytes, Type::Int, Type::String },
    bytes_float = { Type::Bytes, Type::Float, Type::Float },
    bytes_duration = { Type::Bytes, Type::Duration, Type::String },
    bytes_bytes = { Type::Bytes, Type::Bytes, Type::Bytes },
    bytes_string = { Type::Bytes, Type::String, Type::String },
    string_boolean = { Type::String, Type::Boolean, Type::String },
    string_int = { Type::String, Type::Int, Type::String },
    string_float = { Type::String, Type::Float, Type::String },
    string_duration = { Type::String, Type::Duration, Type::String },
    string_bytes = { Type::String, Type::Bytes, Type::String },
    string_string = { Type::String, Type::String, Type::String },
)]
fn detected_field_types_merge_to_what_still_describes_both(left: Type, right: Type, want: Type) {
    check!(left.merge(right) == want, "{left:?} with {right:?}");
}
