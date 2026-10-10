use krabka_traceql::matcher_attributes_match;

use super::{
    MatchScope, RecordBatch, SpanMatcher, TraceqlError, batch_attr_matches, event_values,
    instrumentation_matches, intrinsic_matches, link_values, resource_matches,
};

pub(crate) fn row_matcher_matches(
    batch: &RecordBatch,
    row: usize,
    matcher: &SpanMatcher,
) -> Result<bool, TraceqlError> {
    let is_match = match matcher.scope {
        MatchScope::Event => event_values(batch, row)?
            .iter()
            .any(|event| matcher_attributes_match(&event.attributes, matcher)),
        MatchScope::Link => link_values(batch, row)?
            .iter()
            .any(|link| matcher_attributes_match(&link.attributes, matcher)),
        MatchScope::Intrinsic => intrinsic_matches(batch, row, matcher)?,
        MatchScope::Resource => resource_matches(batch, row, matcher)?,
        MatchScope::Instrumentation => instrumentation_matches(batch, row, matcher)?,
        MatchScope::Both => {
            resource_matches(batch, row, matcher)?
                || batch_attr_matches(batch, row, &matcher.key, matcher.op, &matcher.value)?
        }
        MatchScope::Span => {
            batch_attr_matches(batch, row, &matcher.key, matcher.op, &matcher.value)?
        }
        MatchScope::Parent => true,
    };
    Ok(is_match != matcher.negated)
}
