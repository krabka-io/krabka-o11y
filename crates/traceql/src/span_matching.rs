//! How a [`SpanMatcher`] compares against attribute values, events and
//! links, and how nested event and link tags list their values.
//!
//! Every `SpanStore` evaluates matchers with these functions, so the
//! in-memory store and the block-backed stores agree on `TraceQL` semantics.

use std::collections::BTreeSet;

use krabka_units::{Time, convert::TimeExt};

use crate::{
    engine::bytes_to_hex,
    result::{AttrValue, EventRef, LinkRef, SpanRef, TypedValue},
    span_columns::InputSpan,
    store::{MatchCmp, MatchScope, MatchValue, SpanMatcher},
};

fn matches_ordering(ordering: Option<std::cmp::Ordering>, op: MatchCmp) -> bool {
    match op {
        MatchCmp::Eq => ordering == Some(std::cmp::Ordering::Equal),
        MatchCmp::Neq => ordering != Some(std::cmp::Ordering::Equal),
        MatchCmp::Lt => ordering == Some(std::cmp::Ordering::Less),
        MatchCmp::Lte => matches!(
            ordering,
            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
        ),
        MatchCmp::Gt => ordering == Some(std::cmp::Ordering::Greater),
        MatchCmp::Gte => matches!(
            ordering,
            Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
        ),
        MatchCmp::Re | MatchCmp::Nre => false,
    }
}

pub(crate) fn nested_attribute_key_matches(key: &str, tag: &str, scope_prefix: &str) -> bool {
    key == tag || tag.strip_prefix(scope_prefix).is_some_and(|tag| key == tag)
}

/// Whether an absent value satisfies `op` against `expected`.
#[must_use]
pub fn nil_matches(op: MatchCmp, expected: &MatchValue) -> bool {
    matches!((op, expected), (MatchCmp::Eq, MatchValue::Nil))
}

/// Whether `value` satisfies `op` against a boolean `expected`.
#[must_use]
pub fn bool_matches(value: bool, op: MatchCmp, expected: &MatchValue) -> bool {
    let MatchValue::Bool(expected) = expected else {
        return false;
    };
    match op {
        MatchCmp::Eq => value == *expected,
        MatchCmp::Neq => value != *expected,
        MatchCmp::Lt
        | MatchCmp::Lte
        | MatchCmp::Gt
        | MatchCmp::Gte
        | MatchCmp::Re
        | MatchCmp::Nre => false,
    }
}

/// Whether an enumerated intrinsic such as `span:kind` satisfies `op`.
///
/// `enum_value` maps a lowercase name to its integer, so `expected` can be
/// a name or a number.
#[must_use]
pub fn enum_int_matches(
    value: i64,
    op: MatchCmp,
    expected: &MatchValue,
    enum_value: fn(&str) -> Option<i32>,
) -> bool {
    let expected = match expected {
        MatchValue::Regex(regex) => enum_value(&regex.pattern.to_ascii_lowercase()).map(i64::from),
        MatchValue::Str(name) => enum_value(&name.to_ascii_lowercase()).map(i64::from),
        MatchValue::Int(value) => Some(*value),
        MatchValue::Nil => return present_value_matches(op, expected).unwrap_or(false),
        MatchValue::Float(_) | MatchValue::Bool(_) => None,
    };
    expected.is_some_and(|expected| int_matches(value, op, &MatchValue::Int(expected)))
}

/// Whether an event matcher matches a span that has no events.
#[must_use]
pub fn event_matcher_matches_absence(matcher: &SpanMatcher) -> bool {
    let is_match = match matcher.scope {
        MatchScope::Event => nil_matches(matcher.op, &matcher.value),
        MatchScope::Intrinsic => match matcher.key.as_str() {
            "event:name" | "event:timeSinceStart" => {
                nested_presence_matches(false, matcher.op, &matcher.value).unwrap_or(false)
            }
            _ => false,
        },
        _ => false,
    };
    is_match != matcher.negated
}

/// Whether an event matcher matches one event.
#[must_use]
pub fn event_matcher_matches_event(event: &EventRef, matcher: &SpanMatcher) -> bool {
    let is_match = match matcher.scope {
        MatchScope::Event => matcher_attributes_match(&event.attributes, matcher),
        MatchScope::Intrinsic => match matcher.key.as_str() {
            "event:name" => nested_presence_matches(true, matcher.op, &matcher.value)
                .unwrap_or_else(|| string_matches(&event.name, matcher.op, &matcher.value)),
            "event:timeSinceStart" => nested_presence_matches(true, matcher.op, &matcher.value)
                .unwrap_or_else(|| {
                    int_matches(
                        event.time_since_start.nanos_i64(),
                        matcher.op,
                        &matcher.value,
                    )
                }),
            _ => false,
        },
        _ => false,
    };
    is_match != matcher.negated
}

/// Whether `value` satisfies `op` against a float `expected`.
///
/// NaN is unequal to everything and ordered against nothing.
#[must_use]
pub fn float_matches(value: f64, op: MatchCmp, expected: &MatchValue) -> bool {
    let expected = match expected {
        MatchValue::Float(value) => *value,
        _ => return false,
    };
    matches_ordering(value.partial_cmp(&expected), op)
}

/// Whether `value` satisfies `op` against an integer `expected`.
#[must_use]
pub fn int_matches(value: i64, op: MatchCmp, expected: &MatchValue) -> bool {
    if let Some(matches) = present_value_matches(op, expected) {
        return matches;
    }
    let expected = match expected {
        MatchValue::Int(value) => *value,
        _ => return false,
    };
    matches_ordering(Some(value.cmp(&expected)), op)
}

/// Whether a link matcher matches a span that has no links.
#[must_use]
pub fn link_matcher_matches_absence(matcher: &SpanMatcher) -> bool {
    let is_match = match matcher.scope {
        MatchScope::Link => nil_matches(matcher.op, &matcher.value),
        MatchScope::Intrinsic => match matcher.key.as_str() {
            "link:traceID" | "link:spanID" => {
                nested_presence_matches(false, matcher.op, &matcher.value).unwrap_or(false)
            }
            _ => false,
        },
        _ => false,
    };
    is_match != matcher.negated
}

/// Whether a link matcher matches one link.
#[must_use]
pub fn link_matcher_matches_link(link: &LinkRef, matcher: &SpanMatcher) -> bool {
    let is_match = match matcher.scope {
        MatchScope::Link => matcher_attributes_match(&link.attributes, matcher),
        MatchScope::Intrinsic => match matcher.key.as_str() {
            "link:traceID" => nested_presence_matches(true, matcher.op, &matcher.value)
                .unwrap_or_else(|| {
                    string_matches(&bytes_to_hex(&link.trace_id), matcher.op, &matcher.value)
                }),
            "link:spanID" => nested_presence_matches(true, matcher.op, &matcher.value)
                .unwrap_or_else(|| {
                    string_matches(&bytes_to_hex(&link.span_id), matcher.op, &matcher.value)
                }),
            _ => false,
        },
        _ => false,
    };
    is_match != matcher.negated
}

/// Whether `value` satisfies `op` against a string `expected`.
///
/// A regex must match the whole value; an invalid regex matches nothing.
/// Use [`SpanMatcher::prepare_regexes`] before a scan to reuse compiled patterns.
#[must_use]
pub fn string_matches(value: &str, op: MatchCmp, expected: &MatchValue) -> bool {
    let (pattern, compiled) = match expected {
        MatchValue::Str(pattern) => (pattern.as_str(), None),
        MatchValue::Regex(regex) => (regex.pattern.as_str(), Some(&regex.compiled)),
        _ => return false,
    };
    match op {
        MatchCmp::Eq => value == pattern,
        MatchCmp::Neq => value != pattern,
        MatchCmp::Re | MatchCmp::Nre => {
            let fallback;
            let compiled = if let Some(compiled) = compiled {
                compiled
            } else {
                fallback = regex::Regex::new(&format!("^(?:{pattern})$")).ok();
                &fallback
            };
            compiled
                .as_ref()
                .is_some_and(|regex| regex.is_match(value) != (op == MatchCmp::Nre))
        }
        MatchCmp::Lt | MatchCmp::Lte | MatchCmp::Gt | MatchCmp::Gte => false,
    }
}

// Attr Matches
/// Whether one attribute value satisfies `op` against `expected`.
///
/// An empty array is nil and a one-element array is its element. A longer
/// array matches a negated operator only when every element does, and any
/// other operator when one element does. An unsupported value never matches.
#[must_use]
pub fn attr_matches(value: &AttrValue, op: MatchCmp, expected: &MatchValue) -> bool {
    if matches!(value, AttrValue::Unsupported(_)) {
        return false;
    }
    if let AttrValue::Array(values) = value {
        if values.is_empty() {
            return matches!((op, expected), (MatchCmp::Eq, MatchValue::Nil));
        }
        if values.len() == 1 {
            return attr_matches(&values[0], op, expected);
        }
    }
    if let Some(matches) = present_value_matches(op, expected) {
        return matches;
    }
    match value {
        AttrValue::Unsupported(_) => false,
        AttrValue::Array(values) => {
            if matches!(op, MatchCmp::Neq | MatchCmp::Nre) {
                values.iter().all(|value| attr_matches(value, op, expected))
            } else {
                values.iter().any(|value| attr_matches(value, op, expected))
            }
        }
        AttrValue::Str(value) => string_matches(value, op, expected),
        AttrValue::Int(value) => int_matches(*value, op, expected),
        AttrValue::Float(value) => float_matches(*value, op, expected),
        AttrValue::Bool(value) => bool_matches(*value, op, expected),
    }
}

// Attr Values Match
use crate::result::normalized_attr_value;

/// Whether the values one key holds satisfy `op` against `expected`.
///
/// With no comparable value the key is absent, and only `= nil` matches.
#[must_use]
pub fn attr_values_match(values: &[&AttrValue], op: MatchCmp, expected: &MatchValue) -> bool {
    let values = values
        .iter()
        .filter_map(|value| normalized_attr_value(value))
        .collect::<Vec<_>>();
    if values.is_empty() {
        return nil_matches(op, expected);
    }
    if let Some(matches) = present_value_matches(op, expected) {
        return matches;
    }
    match op {
        MatchCmp::Neq | MatchCmp::Nre => {
            values.iter().all(|value| attr_matches(value, op, expected))
        }
        MatchCmp::Eq
        | MatchCmp::Re
        | MatchCmp::Lt
        | MatchCmp::Lte
        | MatchCmp::Gt
        | MatchCmp::Gte => values.iter().any(|value| attr_matches(value, op, expected)),
    }
}

// Collect Event Values
/// Adds the `(type, value)` pairs that `tag` lists across `events`.
///
/// `tag` is an event intrinsic, an event attribute, or an attribute key
/// with the `event.` scope prefix.
pub fn collect_event_values(events: &[EventRef], tag: &str, values: &mut BTreeSet<TypedValue>) {
    for event in events {
        match tag {
            "event:name" => {
                values.insert(TypedValue {
                    type_: "string".to_string(),
                    value: event.name.clone(),
                });
            }
            "event:timeSinceStart" => {
                values.insert(TypedValue {
                    type_: "duration".to_string(),
                    value: event.time_since_start.nanos_i64().to_string(),
                });
            }
            _ => {}
        }
        values.extend(
            event
                .attributes
                .iter()
                .filter(|attribute| nested_attribute_key_matches(&attribute.key, tag, "event."))
                .flat_map(|attribute| typed_value_parts(&attribute.value)),
        );
    }
}

// Collect Link Values
/// Adds the `(type, value)` pairs that `tag` lists across `links`.
///
/// `tag` is a link intrinsic, a link attribute, or an attribute key with
/// the `link.` scope prefix.
pub fn collect_link_values(links: &[LinkRef], tag: &str, values: &mut BTreeSet<TypedValue>) {
    for link in links {
        match tag {
            "link:traceID" => {
                values.insert(TypedValue {
                    type_: "string".to_string(),
                    value: bytes_to_hex(&link.trace_id),
                });
            }
            "link:spanID" => {
                values.insert(TypedValue {
                    type_: "string".to_string(),
                    value: bytes_to_hex(&link.span_id),
                });
            }
            _ => {}
        }
        values.extend(
            link.attributes
                .iter()
                .filter(|attribute| nested_attribute_key_matches(&attribute.key, tag, "link."))
                .flat_map(|attribute| typed_value_parts(&attribute.value)),
        );
    }
}

// Collect Span Field Values
/// Adds the `(type, value)` pair that `tag` lists for one span, when `tag`
/// is a span or instrumentation intrinsic read off the span alone.
///
/// The nested-set intrinsics (`span:childCount`, `span:nestedSet*`) need the
/// rest of the trace, so the caller resolves those itself.
pub fn collect_span_field_values(
    span: SpanIntrinsicFields<'_>,
    tag: &str,
    values: &mut BTreeSet<TypedValue>,
) {
    match tag {
        "span:duration" => {
            values.insert(TypedValue {
                type_: "duration".to_string(),
                value: span.duration.nanos_i64().to_string(),
            });
        }
        "span:id" => {
            values.insert(TypedValue {
                type_: "string".to_string(),
                value: bytes_to_hex(&span.span_id),
            });
        }
        "span:kind" => {
            values.insert(TypedValue {
                type_: "int".to_string(),
                value: span.kind.to_string(),
            });
        }
        "span:name" => {
            values.insert(TypedValue {
                type_: "string".to_string(),
                value: span.name.to_string(),
            });
        }
        "span:parentID" => {
            if let Some(parent_id) = span.parent_span_id {
                values.insert(TypedValue {
                    type_: "string".to_string(),
                    value: bytes_to_hex(&parent_id),
                });
            }
        }
        "span:status" => {
            values.insert(TypedValue {
                type_: "int".to_string(),
                value: span.status_code.to_string(),
            });
        }
        "span:statusMessage" if !span.status_message.is_empty() => {
            values.insert(TypedValue {
                type_: "string".to_string(),
                value: span.status_message.to_string(),
            });
        }
        "instrumentation:name" if !span.instrumentation_name.is_empty() => {
            values.insert(TypedValue {
                type_: "string".to_string(),
                value: span.instrumentation_name.to_string(),
            });
        }
        "instrumentation:version" if !span.instrumentation_version.is_empty() => {
            values.insert(TypedValue {
                type_: "string".to_string(),
                value: span.instrumentation_version.to_string(),
            });
        }
        _ => {}
    }
}

// Matcher Attributes Match
use crate::result::Attribute;

/// Whether the values `attributes` holds under `matcher.key` satisfy the
/// matcher's operator and operand.
///
/// This ignores `matcher.negated`, and reads every attribute regardless of
/// `matcher.scope`; the caller picks the attribute list the scope names.
#[must_use]
pub fn matcher_attributes_match(attributes: &[Attribute], matcher: &SpanMatcher) -> bool {
    let values = attributes
        .iter()
        .filter(|attribute| attribute.key == matcher.key)
        .map(|attribute| &attribute.value)
        .collect::<Vec<_>>();
    attr_values_match(&values, matcher.op, &matcher.value)
}

// Nested Presence Matches
struct NestedPresenceComparison<'a> {
    operator: MatchCmp,
    expected: &'a MatchValue,
}

/// The answer to a nil comparison on a nested field, or `None` for any
/// other comparison.
///
/// `has_values` says whether the span holds the field at all.
#[must_use]
pub fn nested_presence_matches(
    has_values: bool,
    op: MatchCmp,
    expected: &MatchValue,
) -> Option<bool> {
    match (NestedPresenceComparison {
        operator: op,
        expected,
    }) {
        NestedPresenceComparison {
            operator: MatchCmp::Eq,
            expected: MatchValue::Nil,
        } => Some(!has_values),
        NestedPresenceComparison {
            operator: MatchCmp::Neq,
            expected: MatchValue::Nil,
        } => Some(has_values),
        _ => None,
    }
}

// Present Value Matches
struct PresentValueComparison<'a> {
    operator: MatchCmp,
    expected: &'a MatchValue,
}

/// The answer to a nil comparison on a present value, or `None` for any
/// other comparison.
#[must_use]
pub fn present_value_matches(op: MatchCmp, expected: &MatchValue) -> Option<bool> {
    match (PresentValueComparison {
        operator: op,
        expected,
    }) {
        PresentValueComparison {
            operator: MatchCmp::Eq,
            expected: MatchValue::Nil,
        } => Some(false),
        PresentValueComparison {
            operator: MatchCmp::Neq,
            expected: MatchValue::Nil,
        } => Some(true),
        _ => None,
    }
}

// Span Intrinsic Fields
/// The span intrinsics that a tag-values lookup reads off one span alone,
/// without its position in the trace's nested set.
#[derive(Clone, Copy, Debug)]
pub struct SpanIntrinsicFields<'a> {
    pub span_id: [u8; 8],
    pub parent_span_id: Option<[u8; 8]>,
    pub name: &'a str,
    pub kind: i32,
    pub duration: Time,
    pub status_code: i32,
    pub status_message: &'a str,
    pub instrumentation_name: &'a str,
    pub instrumentation_version: &'a str,
}

macro_rules! impl_span_intrinsic_fields_from {
    ($span:ty) => {
        impl<'a> From<&'a $span> for SpanIntrinsicFields<'a> {
            fn from(span: &'a $span) -> Self {
                Self {
                    span_id: span.span_id,
                    parent_span_id: span.parent_span_id,
                    name: &span.name,
                    kind: span.kind,
                    duration: span.duration,
                    status_code: span.status_code,
                    status_message: &span.status_message,
                    instrumentation_name: &span.instrumentation_name,
                    instrumentation_version: &span.instrumentation_version,
                }
            }
        }
    };
}

impl_span_intrinsic_fields_from!(InputSpan);
impl_span_intrinsic_fields_from!(SpanRef);

// Typed Value Parts
/// Flattens `value` into Tempo's `(type, value)` tag-value pairs, recursing
/// into arrays and dropping the opaque values Tempo does not list.
#[must_use]
pub fn typed_value_parts(value: &AttrValue) -> Vec<TypedValue> {
    if let AttrValue::Array(values) = value {
        return values.iter().flat_map(typed_value_parts).collect();
    }
    if matches!(value, AttrValue::Unsupported(_)) {
        return Vec::new();
    }
    vec![match value {
        AttrValue::Str(v) => TypedValue {
            type_: "string".to_string(),
            value: v.clone(),
        },
        AttrValue::Int(v) => TypedValue {
            type_: "int".to_string(),
            value: v.to_string(),
        },
        AttrValue::Float(v) => TypedValue {
            type_: "float".to_string(),
            value: v.to_string(),
        },

        AttrValue::Bool(v) => TypedValue {
            type_: "bool".to_string(),
            value: v.to_string(),
        },
        AttrValue::Unsupported(_) | AttrValue::Array(_) => unreachable!("arrays handled above"),
    }]
}
