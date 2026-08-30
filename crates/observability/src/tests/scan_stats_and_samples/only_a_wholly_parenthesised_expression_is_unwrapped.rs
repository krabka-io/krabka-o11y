use super::*;

/// `strip_outer_parenthesized_expression` unwraps a query that is wholly
/// parenthesised, and refuses one that merely starts and ends with
/// brackets belonging to different groups -- "(a)+(b)" is not a
/// parenthesised expression, and unwrapping it would produce "a)+(b".
#[test]
pub(crate) fn only_a_wholly_parenthesised_expression_is_unwrapped() {
    let strip = super::super::prelude::strip_outer_parenthesized_expression;

    check!(strip("(a)") == Some("a"));
    check!(strip("  (a)  ") == Some("a"), "the query is trimmed first");
    check!(strip("( a )") == Some("a"), "and so are the contents");
    check!(strip("((a))") == Some("(a)"), "one layer at a time");
    check!(strip("(a+b)") == Some("a+b"));

    // The brackets must be the SAME pair. This is the case that a naive
    // starts-with/ends-with check gets wrong.
    check!(strip("(a)+(b)").is_none());
    check!(strip("(a)(b)").is_none());

    // Not parenthesised at all. "a(b)" matters most: it ends with a
    // bracket whose opener is not the first character, so a precheck
    // requiring only ONE of the two ends to match would unwrap it to the
    // nonsense "(b".
    check!(strip("a(b)").is_none());
    check!(strip("a").is_none());
    check!(strip("(a").is_none());
    check!(strip("a)").is_none());
    check!(strip("").is_none());

    // Unbalanced inside. Note the `checked_sub` guarding the depth counter
    // is unreachable: a leading `)` would need the opening precheck to have
    // passed, which requires a leading `(`. Replacing it with a saturating
    // subtraction is an equivalent mutation, not a gap.
    check!(strip("(a))").is_none());
    check!(strip("((a)").is_none());

    // A parenthesis inside a string is text, not structure.
    check!(strip(r#"({app="("})"#) == Some(r#"{app="("}"#));
}
