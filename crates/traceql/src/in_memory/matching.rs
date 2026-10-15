use super::{
    AttrValue, EVENT_ATTR_PREFIX, EventRef, InputSpan, LINK_ATTR_PREFIX, LinkRef, MatchCmp,
    MatchScope, MatchValue, NestedSet, SpanMatcher, SpanRef, StoredTrace, attr_values_match,
    enum_int_matches, int_matches,
    event_matcher_matches_absence, event_matcher_matches_event, is_event_matcher,
    is_link_matcher, link_matcher_matches_absence, link_matcher_matches_link,
    matcher_attributes_match, nested_presence_matches, nil_matches, resource_matches,
    string_matches,
};

use crate::{engine::bytes_to_hex, span_enum_codes::{kind_enum_value, status_enum_value}};
use krabka_units::convert::TimeExt as _;

// Instrumentation Matches
pub(crate) fn instrumentation_matches(span: &InputSpan, matcher: &SpanMatcher) -> bool {
    match matcher.key.as_str() {
        "name" | "instrumentation:name" => {
            string_matches(&span.instrumentation_name, matcher.op, &matcher.value)
        }
        "version" | "instrumentation:version" => {
            string_matches(&span.instrumentation_version, matcher.op, &matcher.value)
        }
        _ => span_attr_matches(
            span,
            &format!("{}{}", crate::INSTRUMENTATION_ATTR_PREFIX, matcher.key),
            matcher.op,
            &matcher.value,
        ),
    }
}

// Intrinsic Matches
pub(crate) fn intrinsic_matches(
    trace: &StoredTrace,
    span: &InputSpan,
    nested_sets: &[NestedSet],
    idx: usize,
    matcher: &SpanMatcher,
) -> bool {
    match matcher.key.as_str() {
        "name" | "span:name" => string_matches(&span.name, matcher.op, &matcher.value),
        "event:name" => {
            nested_presence_matches(!span.events.is_empty(), matcher.op, &matcher.value)
                .unwrap_or_else(|| {
                    span.events
                        .iter()
                        .any(|event| string_matches(&event.name, matcher.op, &matcher.value))
                })
        }
        "event:timeSinceStart" => {
            nested_presence_matches(!span.events.is_empty(), matcher.op, &matcher.value)
                .unwrap_or_else(|| {
                    span.events.iter().any(|event| {
                        int_matches(
                            event.time_since_start.nanos_i64(),
                            matcher.op,
                            &matcher.value,
                        )
                    })
                })
        }
        "link:traceID" => {
            nested_presence_matches(!span.links.is_empty(), matcher.op, &matcher.value)
                .unwrap_or_else(|| {
                    span.links.iter().any(|link| {
                        string_matches(&bytes_to_hex(&link.trace_id), matcher.op, &matcher.value)
                    })
                })
        }
        "link:spanID" => {
            nested_presence_matches(!span.links.is_empty(), matcher.op, &matcher.value)
                .unwrap_or_else(|| {
                    span.links.iter().any(|link| {
                        string_matches(&bytes_to_hex(&link.span_id), matcher.op, &matcher.value)
                    })
                })
        }
        "trace:id" => string_matches(&bytes_to_hex(&trace.trace_id), matcher.op, &matcher.value),
        "trace:rootService" => string_matches(&trace.root_service_name, matcher.op, &matcher.value),
        "trace:rootName" => string_matches(&trace.root_span_name, matcher.op, &matcher.value),
        "trace:duration" => {
            int_matches(trace.trace_duration.nanos_i64(), matcher.op, &matcher.value)
        }
        "duration" | "span:duration" => {
            int_matches(span.duration.nanos_i64(), matcher.op, &matcher.value)
        }
        "span:id" => string_matches(&bytes_to_hex(&span.span_id), matcher.op, &matcher.value),
        "span:parentID" => span.parent_span_id.map_or_else(
            || nil_matches(matcher.op, &matcher.value),
            |parent| string_matches(&bytes_to_hex(&parent), matcher.op, &matcher.value),
        ),
        "kind" | "span:kind" => enum_int_matches(
            i64::from(span.kind),
            matcher.op,
            &matcher.value,
            kind_enum_value,
        ),
        "status" | "span:status" => enum_int_matches(
            i64::from(span.status_code),
            matcher.op,
            &matcher.value,
            status_enum_value,
        ),
        "statusMessage" | "span:statusMessage" => {
            string_matches(&span.status_message, matcher.op, &matcher.value)
        }
        "span:childCount" => int_matches(
            i64::from(child_count_for(nested_sets, idx)),
            matcher.op,
            &matcher.value,
        ),
        "span:nestedSetLeft" => nested_sets
            .get(idx)
            .is_some_and(|nested| int_matches(i64::from(nested.left), matcher.op, &matcher.value)),
        "span:nestedSetRight" => nested_sets
            .get(idx)
            .is_some_and(|nested| int_matches(i64::from(nested.right), matcher.op, &matcher.value)),
        "span:nestedSetParent" => nested_sets.get(idx).is_some_and(|nested| {
            int_matches(i64::from(nested.parent_id), matcher.op, &matcher.value)
        }),
        "instrumentation:name" => {
            string_matches(&span.instrumentation_name, matcher.op, &matcher.value)
        }
        "instrumentation:version" => {
            string_matches(&span.instrumentation_version, matcher.op, &matcher.value)
        }
        _ => true,
    }
}

// Matcher Matches
pub(crate) fn matcher_matches(
    trace: &StoredTrace,
    span: &InputSpan,
    nested_sets: &[NestedSet],
    idx: usize,
    matcher: &SpanMatcher,
) -> bool {
    let is_match = match matcher.scope {
        MatchScope::Event => span
            .events
            .iter()
            .any(|event| matcher_attributes_match(&event.attributes, matcher)),
        MatchScope::Link => span
            .links
            .iter()
            .any(|link| matcher_attributes_match(&link.attributes, matcher)),
        MatchScope::Intrinsic => intrinsic_matches(trace, span, nested_sets, idx, matcher),
        MatchScope::Resource => resource_matches(trace, matcher),
        MatchScope::Instrumentation => instrumentation_matches(span, matcher),
        MatchScope::Both => {
            resource_matches(trace, matcher)
                || span_attr_matches(span, &matcher.key, matcher.op, &matcher.value)
        }
        MatchScope::Span => span_attr_matches(span, &matcher.key, matcher.op, &matcher.value),
        MatchScope::Parent => true,
    };
    is_match != matcher.negated
}

// Matching Events For Scan
pub(crate) fn matching_events_for_scan<'a>(
    span: &'a InputSpan,
    matchers: &[SpanMatcher],
) -> Vec<Option<&'a EventRef>> {
    let event_matchers = matchers
        .iter()
        .filter(|matcher| is_event_matcher(matcher))
        .collect::<Vec<_>>();
    if event_matchers.is_empty() {
        return vec![span.events.first()];
    }
    if span.events.is_empty() {
        return vec![None];
    }
    span.events
        .iter()
        .filter(|event| {
            event_matchers
                .iter()
                .all(|matcher| event_matcher_matches_event(event, matcher))
        })
        .take(1)
        .map(Some)
        .collect()
}

// Matching Links For Scan
pub(crate) fn matching_links_for_scan<'a>(
    span: &'a InputSpan,
    matchers: &[SpanMatcher],
) -> Vec<Option<&'a LinkRef>> {
    let link_matchers = matchers
        .iter()
        .filter(|matcher| is_link_matcher(matcher))
        .collect::<Vec<_>>();
    if link_matchers.is_empty() {
        return vec![span.links.first()];
    }
    if span.links.is_empty() {
        return vec![None];
    }
    span.links
        .iter()
        .filter(|link| {
            link_matchers
                .iter()
                .all(|matcher| link_matcher_matches_link(link, matcher))
        })
        .take(1)
        .map(Some)
        .collect()
}

// Nested Attr Value
pub(crate) fn nested_attr_value<'a>(
    key: &str,
    span: &'a InputSpan,
    event: Option<&'a EventRef>,
    link: Option<&'a LinkRef>,
) -> Option<&'a AttrValue> {
    if let Some(key) = key.strip_prefix(EVENT_ATTR_PREFIX) {
        return event.and_then(|event| {
            event
                .attributes
                .iter()
                .find(|attribute| attribute.key == key)
                .map(|attribute| &attribute.value)
        });
    }
    if let Some(key) = key.strip_prefix(LINK_ATTR_PREFIX) {
        return link.and_then(|link| {
            link.attributes
                .iter()
                .find(|attribute| attribute.key == key)
                .map(|attribute| &attribute.value)
        });
    }
    span.attrs
        .iter()
        .find(|attribute| attribute.key == key)
        .map(|attribute| &attribute.value)
}

// Nested Event Matchers Match
pub(crate) fn nested_event_matchers_match(span: &InputSpan, matchers: &[SpanMatcher]) -> bool {
    let event_matchers = matchers
        .iter()
        .filter(|matcher| is_event_matcher(matcher))
        .collect::<Vec<_>>();
    if event_matchers.is_empty() {
        return true;
    }
    if span.events.is_empty() {
        return event_matchers
            .iter()
            .all(|matcher| event_matcher_matches_absence(matcher));
    }
    span.events.iter().any(|event| {
        event_matchers
            .iter()
            .all(|matcher| event_matcher_matches_event(event, matcher))
    })
}

// Nested Link Matchers Match
pub(crate) fn nested_link_matchers_match(span: &InputSpan, matchers: &[SpanMatcher]) -> bool {
    let link_matchers = matchers
        .iter()
        .filter(|matcher| is_link_matcher(matcher))
        .collect::<Vec<_>>();
    if link_matchers.is_empty() {
        return true;
    }
    if span.links.is_empty() {
        return link_matchers
            .iter()
            .all(|matcher| link_matcher_matches_absence(matcher));
    }
    span.links.iter().any(|link| {
        link_matchers
            .iter()
            .all(|matcher| link_matcher_matches_link(link, matcher))
    })
}

// Span Attr Matches
pub(crate) fn span_attr_matches(
    span: &InputSpan,
    key: &str,
    op: MatchCmp,
    expected: &MatchValue,
) -> bool {
    let values = span
        .attrs
        .iter()
        .filter(|attribute| attribute.key == key)
        .map(|attribute| &attribute.value)
        .collect::<Vec<_>>();
    attr_values_match(&values, op, expected)
}

// Span Matches
pub(crate) fn span_matches(
    trace: &StoredTrace,
    span: &InputSpan,
    nested_sets: &[NestedSet],
    idx: usize,
    matchers: &[SpanMatcher],
) -> bool {
    if !nested_event_matchers_match(span, matchers) || !nested_link_matchers_match(span, matchers) {
        return false;
    }
    matchers
        .iter()
        .filter(|matcher| !is_event_matcher(matcher) && !is_link_matcher(matcher))
        .all(|matcher| matcher_matches(trace, span, nested_sets, idx, matcher))
}

// Span Ref
pub(crate) fn span_ref(span: &InputSpan, nested: &NestedSet) -> SpanRef {
    SpanRef {
        span_id: span.span_id,
        parent_span_id: span.parent_span_id,
        name: span.name.clone(),
        kind: span.kind,
        nested_set_left: nested.left,
        nested_set_right: nested.right,
        nested_set_parent: nested.parent_id,
        start_time_unix_nano: u64::try_from(span.start_unix_nano).unwrap_or(0),
        duration: span.duration,
        status_code: span.status_code,
        status_message: span.status_message.clone(),
        instrumentation_name: span.instrumentation_name.clone(),
        instrumentation_version: span.instrumentation_version.clone(),
        resource_attributes: Vec::new(),
        attributes: span.attrs.clone(),
        events: span.events.clone(),
        links: span.links.clone(),
    }
}

// Child Count For
pub(crate) fn child_count_for(nested_sets: &[NestedSet], idx: usize) -> i32 {
    let Some(nested) = nested_sets.get(idx) else {
        return 0;
    };
    i32::try_from(
        nested_sets
            .iter()
            .filter(|other| other.parent_id == nested.left)
            .count(),
    )
    .unwrap_or(i32::MAX)
}
