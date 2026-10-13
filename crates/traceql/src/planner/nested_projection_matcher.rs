use super::{Field, Intrinsic, MatchCmp, MatchScope, MatchValue, Scope, SpanMatcher};

pub(crate) fn nested_projection_matcher(field: &Field) -> Option<SpanMatcher> {
    let matcher = match &field.scope {
        Scope::Event => projection_matcher(MatchScope::Event, field.key.clone()),
        Scope::Link => projection_matcher(MatchScope::Link, field.key.clone()),
        Scope::Intrinsic(Intrinsic::EventName) => {
            projection_matcher(MatchScope::Intrinsic, "event:name".into())
        }
        Scope::Intrinsic(Intrinsic::EventTimeSinceStart) => {
            projection_matcher(MatchScope::Intrinsic, "event:timeSinceStart".into())
        }
        Scope::Intrinsic(Intrinsic::LinkTraceId) => {
            projection_matcher(MatchScope::Intrinsic, "link:traceID".into())
        }
        Scope::Intrinsic(Intrinsic::LinkSpanId) => {
            projection_matcher(MatchScope::Intrinsic, "link:spanID".into())
        }
        // A by()/select field on a regular span or resource attribute must be
        // projected too: grouping reads it as a column (`GROUP BY attr.X`), but
        // the scan otherwise materializes attrs only from the selector's filter
        // matchers, so `rate() by(span.http.method)` fails with "missing column
        // attr.http.method". This is projection-only — projection_matchers do not
        // filter (the scan filters on the attr arrays separately), so spans
        // lacking the attribute still appear under the nil group.
        Scope::Both => projection_matcher(MatchScope::Both, field.key.clone()),
        Scope::Span => projection_matcher(MatchScope::Span, field.key.clone()),
        Scope::Resource => projection_matcher(MatchScope::Resource, field.key.clone()),
        Scope::Instrumentation => {
            projection_matcher(MatchScope::Instrumentation, field.key.clone())
        }
        Scope::Parent | Scope::Intrinsic(_) => return None,
    };
    Some(matcher)
}

fn projection_matcher(scope: MatchScope, key: String) -> SpanMatcher {
    SpanMatcher {
        scope,
        key,
        op: MatchCmp::Neq,
        value: MatchValue::Nil,
        negated: false,
    }
}
