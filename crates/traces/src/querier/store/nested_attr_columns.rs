use super::*;

pub(crate) fn nested_attr_columns(matchers: &[SpanMatcher]) -> Vec<(String, NestedAttrColumn<'_>)> {
    let mut out = Vec::new();
    for matcher in matchers {
        let (scope, prefix) = match matcher.scope {
            MatchScope::Event => (NestedAttrScope::Event, EVENT_ATTR_PREFIX),
            MatchScope::Link => (NestedAttrScope::Link, LINK_ATTR_PREFIX),
            MatchScope::Both
            | MatchScope::Span
            | MatchScope::Resource
            | MatchScope::Parent
            | MatchScope::Instrumentation
            | MatchScope::Intrinsic => continue,
        };
        let column = format!("{ATTR_PREFIX}{prefix}{}", matcher.key);
        if out.iter().any(|(existing, _)| existing == &column) {
            continue;
        }
        out.push((
            column,
            NestedAttrColumn {
                scope,
                key: &matcher.key,
            },
        ));
    }
    out
}
