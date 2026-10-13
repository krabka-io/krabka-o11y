use super::*;

/// `parse_vector_group_modifier` returns the modifier it read and how many
/// bytes it consumed. The length is the half that matters: a caller
/// resumes from it, so an off-by-one there re-reads a character or skips
/// one, and the returned text alone would look correct either way.
#[test]
pub(crate) fn a_vector_group_modifier_reports_what_it_consumed() {
    let parse = super::super::prelude::parse_vector_group_modifier;
    let matches = |actual: Option<crate::http::params_format::ParsedValueAt<String>>,
                   value: &str,
                   position: usize| {
        actual.is_some_and(|parsed| parsed.value == value && parsed.position == position)
    };

    // Bare, with the length being the whole modifier.
    check!(matches(parse("group_left", 0), "group_left", 10));
    check!(matches(parse("group_right", 0), "group_right", 11));

    // With labels, the length covers the parentheses too.
    check!(matches(parse("group_left(a)", 0), "group_left (a)", 13));
    check!(matches(
        parse("group_right(a,b)", 0),
        "group_right (a,b)",
        16
    ));

    // Empty parentheses are consumed but add no labels.
    check!(matches(parse("group_left()", 0), "group_left", 12));

    // The length is relative to the whole query, not to the slice read.
    check!(matches(parse("x group_left", 2), "group_left", 12));
    check!(matches(parse("x group_left(a)", 2), "group_left (a)", 15));

    // Trailing input is left for the caller rather than swallowed.
    check!(matches(parse("group_left(a) foo", 0), "group_left (a)", 13));

    // An unclosed parenthesis is not a modifier at all.
    check!(parse("group_left(a", 0) == None);
    check!(parse("nothing", 0) == None);
    check!(parse("", 0) == None);
}
