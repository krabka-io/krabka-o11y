use super::*;

/// `parse_vector_matching_modifier` reads an `on(...)`/`ignoring(...)`
/// clause and returns BOTH the rendered modifier and the position just
/// past it. The position is what the caller resumes from, so an
/// off-by-one there leaves a stray bracket in the rest of the query --
/// each case checks the remainder, not just the modifier.
#[test]
pub(crate) fn a_vector_matching_modifier_reports_where_it_ended() {
    let parse = super::super::prelude::parse_vector_matching_modifier;
    let after = |query: &str, position: usize| {
        parse(query, position).map(|(modifier, end)| (modifier, query[end..].to_string()))
    };

    check!(
        after("on(job) foo", 0) == Some(("on (job)".to_string(), " foo".to_string())),
        "the remainder starts after the closing bracket"
    );
    check!(
        after("ignoring(pod) foo", 0) == Some(("ignoring (pod)".to_string(), " foo".to_string()))
    );
    check!(after("on(a,b) foo", 0) == Some(("on (a,b)".to_string(), " foo".to_string())));
    check!(
        after("on() foo", 0) == Some(("on ()".to_string(), " foo".to_string())),
        "an empty label list is still a modifier"
    );

    // Parsing from part-way in, which is how the caller uses it.
    check!(
        after("up on(job) foo", 3) == Some(("on (job)".to_string(), " foo".to_string())),
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
