use super::{
    ATTR_PREFIX, EVENT_ATTR_PREFIX, LINK_ATTR_PREFIX, MatchScope, NestedAttrColumn,
    NestedAttrScope, SpanMatcher, nested_attr_column::NestedAttrColumnProjection,
};

pub(crate) fn nested_attr_columns(matchers: &[SpanMatcher]) -> Vec<NestedAttrColumnProjection<'_>> {
    struct NestedAttrSelector {
        scope: NestedAttrScope,
        prefix: &'static str,
    }
    let mut out: Vec<NestedAttrColumnProjection<'_>> = Vec::new();
    for matcher in matchers {
        let selector = match matcher.scope {
            MatchScope::Event => NestedAttrSelector {
                scope: NestedAttrScope::Event,
                prefix: EVENT_ATTR_PREFIX,
            },
            MatchScope::Link => NestedAttrSelector {
                scope: NestedAttrScope::Link,
                prefix: LINK_ATTR_PREFIX,
            },
            MatchScope::Both
            | MatchScope::Span
            | MatchScope::Resource
            | MatchScope::Parent
            | MatchScope::Instrumentation
            | MatchScope::Intrinsic => continue,
        };
        let column = format!("{ATTR_PREFIX}{}{}", selector.prefix, matcher.key);
        if out.iter().any(|existing| existing.column == column) {
            continue;
        }
        out.push(NestedAttrColumnProjection {
            column,
            attribute: NestedAttrColumn {
                scope: selector.scope,
                key: &matcher.key,
            },
        });
    }
    out
}
