use super::*;

pub(crate) fn nested_projection_matcher(field: &Field) -> Option<SpanMatcher> {
    let (scope, key) = match &field.scope {
        Scope::Event => (MatchScope::Event, field.key.clone()),
        Scope::Link => (MatchScope::Link, field.key.clone()),
        Scope::Intrinsic(Intrinsic::EventName) => (MatchScope::Intrinsic, "event:name".into()),
        Scope::Intrinsic(Intrinsic::EventTimeSinceStart) => {
            (MatchScope::Intrinsic, "event:timeSinceStart".into())
        }
        Scope::Intrinsic(Intrinsic::LinkTraceId) => (MatchScope::Intrinsic, "link:traceID".into()),
        Scope::Intrinsic(Intrinsic::LinkSpanId) => (MatchScope::Intrinsic, "link:spanID".into()),
        // A by()/select field on a regular span or resource attribute must be
        // projected too: grouping reads it as a column (`GROUP BY attr.X`), but
        // the scan otherwise materializes attrs only from the selector's filter
        // matchers, so `rate() by(span.http.method)` fails with "missing column
        // attr.http.method". This is projection-only — projection_matchers do not
        // filter (the scan filters on the attr arrays separately), so spans
        // lacking the attribute still appear under the nil group.
        Scope::Both => (MatchScope::Both, field.key.clone()),
        Scope::Span => (MatchScope::Span, field.key.clone()),
        Scope::Resource => (MatchScope::Resource, field.key.clone()),
        Scope::Parent | Scope::Instrumentation | Scope::Intrinsic(_) => return None,
    };
    Some(SpanMatcher {
        scope,
        key,
        op: MatchCmp::Neq,
        value: MatchValue::Nil,
        negated: false,
    })
}
