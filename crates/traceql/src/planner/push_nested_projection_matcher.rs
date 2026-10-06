use super::{Field, Scope, SpanMatcher, nested_projection_matcher};

pub(crate) fn push_nested_projection_matcher(out: &mut Vec<SpanMatcher>, field: &Field) {
    if field.scope == Scope::Both {
        for scope in [Scope::Event, Scope::Link, Scope::Instrumentation] {
            let scoped = Field {
                scope,
                key: field.key.clone(),
            };
            push_nested_projection_matcher(out, &scoped);
        }
    }
    let Some(matcher) = nested_projection_matcher(field) else {
        return;
    };
    if !out.contains(&matcher) {
        out.push(matcher);
    }
}
