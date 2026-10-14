use super::*;

/// `parse_vector_matching_modifier` reads an `on(...)`/`ignoring(...)`
/// clause and returns BOTH the rendered modifier and the position just
/// past it. The position is what the caller resumes from, so an
/// off-by-one there leaves a stray bracket in the rest of the query --
/// each case checks the remainder, not just the modifier.
#[test]
pub(crate) fn a_vector_matching_modifier_reports_where_it_ended() {
    let parse = super::super::prelude::parse_vector_matching_modifier;
    #[derive(Debug, Eq, PartialEq)]
    struct ModifierRemainder {
        modifier: String,
        remainder: String,
    }
    let after = |query: &str, position: usize| {
        parse(query, position).map(|parsed| ModifierRemainder {
            modifier: parsed.value,
            remainder: query[parsed.position..].to_string(),
        })
    };
    let expected = |modifier: &str, remainder: &str| ModifierRemainder {
        modifier: modifier.to_string(),
        remainder: remainder.to_string(),
    };

    check!(
        after("on(job) foo", 0) == Some(expected("on (job)", " foo")),
        "the remainder starts after the closing bracket"
    );
    check!(after("ignoring(pod) foo", 0) == Some(expected("ignoring (pod)", " foo")));
    check!(after("on(a,b) foo", 0) == Some(expected("on (a,b)", " foo")));
    check!(
        after("on() foo", 0) == Some(expected("on ()", " foo")),
        "an empty label list is still a modifier"
    );

    // Parsing from part-way in, which is how the caller uses it.
    check!(
        after("up on(job) foo", 3) == Some(expected("on (job)", " foo")),
        "the position is an offset into the whole query"
    );

    // Not a modifier at this position.
    check!(parse("foo on(job)", 0).is_none());
    check!(parse("", 0).is_none());
    // The bracket must follow immediately: a space between is not this
    // spelling, and neither is an unclosed list.
    check!(parse("on (job)", 0).is_none());
    check!(parse("on(job", 0).is_none());
}
