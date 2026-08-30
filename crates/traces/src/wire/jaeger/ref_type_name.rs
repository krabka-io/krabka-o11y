
pub(crate) fn ref_type_name(ref_type: i32) -> &'static str {
    match ref_type {
        0 => "child_of",
        1 => "follows_from",
        _ => "reference",
    }
}
