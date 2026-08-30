use super::{Field, SpanMatcher, nested_projection_matcher};

pub(crate) fn push_nested_projection_matcher(out: &mut Vec<SpanMatcher>, field: &Field) {
    let Some(matcher) = nested_projection_matcher(field) else {
        return;
    };
    if !out.contains(&matcher) {
        out.push(matcher);
    }
}
