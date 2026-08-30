use super::{Field, Intrinsic, MatchCmp, MatchScope, MatchValue, Scope, SpanMatcher};

pub(crate) fn nested_metric_projection_matcher(field: &Field) -> Option<SpanMatcher> {
    let (scope, key) = match &field.scope {
        Scope::Event => (MatchScope::Event, field.key.clone()),
        Scope::Link => (MatchScope::Link, field.key.clone()),
        Scope::Intrinsic(Intrinsic::EventName) => (MatchScope::Intrinsic, "event:name".into()),
        Scope::Intrinsic(Intrinsic::EventTimeSinceStart) => {
            (MatchScope::Intrinsic, "event:timeSinceStart".into())
        }
        Scope::Intrinsic(Intrinsic::LinkTraceId) => (MatchScope::Intrinsic, "link:traceID".into()),
        Scope::Intrinsic(Intrinsic::LinkSpanId) => (MatchScope::Intrinsic, "link:spanID".into()),
        // A metric `by()`/value field on a regular span or resource attribute
        // must be projected so the store materializes its `attr.<key>` column for
        // GROUP BY — otherwise `rate() by(span.http.method)` fails with "missing
        // column attr.http.method". Projection-only (does not filter), so spans
        // lacking the attribute stay in the nil group.
        Scope::Both => (MatchScope::Both, field.key.clone()),
        Scope::Span => (MatchScope::Span, field.key.clone()),
        Scope::Resource => (MatchScope::Resource, field.key.clone()),
        Scope::Instrumentation => (MatchScope::Instrumentation, field.key.clone()),
        Scope::Parent | Scope::Intrinsic(_) => return None,
    };
    Some(SpanMatcher {
        scope,
        key,
        op: MatchCmp::Neq,
        value: MatchValue::Nil,
        negated: false,
    })
}
