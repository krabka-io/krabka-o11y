use super::*;

/// `parse_vector_group_modifier` returns the modifier it read and how many
/// bytes it consumed. The length is the half that matters: a caller
/// resumes from it, so an off-by-one there re-reads a character or skips
/// one, and the returned text alone would look correct either way.
#[test]
pub(crate) fn a_vector_group_modifier_reports_what_it_consumed() {
    let parse = super::super::prelude::parse_vector_group_modifier;

    // Bare, with the length being the whole modifier.
    check!(parse("group_left", 0) == Some(("group_left".to_string(), 10)));
    check!(parse("group_right", 0) == Some(("group_right".to_string(), 11)));

    // With labels, the length covers the parentheses too.
    check!(parse("group_left(a)", 0) == Some(("group_left (a)".to_string(), 13)));
    check!(parse("group_right(a,b)", 0) == Some(("group_right (a,b)".to_string(), 16)));

    // Empty parentheses are consumed but add no labels.
    check!(parse("group_left()", 0) == Some(("group_left".to_string(), 12)));

    // The length is relative to the whole query, not to the slice read.
    check!(parse("x group_left", 2) == Some(("group_left".to_string(), 12)));
    check!(parse("x group_left(a)", 2) == Some(("group_left (a)".to_string(), 15)));

    // Trailing input is left for the caller rather than swallowed.
    check!(parse("group_left(a) foo", 0) == Some(("group_left (a)".to_string(), 13)));

    // An unclosed parenthesis is not a modifier at all.
    check!(parse("group_left(a", 0) == None);
    check!(parse("nothing", 0) == None);
    check!(parse("", 0) == None);
}
