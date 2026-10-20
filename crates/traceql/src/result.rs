//! Tempo-shaped `TraceQL` result model.

use std::collections::BTreeSet;

use krabka_units::{ByteSize, Time};

/// Accumulates catalog keys by `TraceQL` tag scope while a store scans spans.
#[derive(Debug, Default)]
pub struct TagNameSets {
    pub resource: BTreeSet<String>,
    pub span: BTreeSet<String>,
    pub event: BTreeSet<String>,
    pub link: BTreeSet<String>,
    pub instrumentation: BTreeSet<String>,
}

impl TagNameSets {
    pub fn add_instrumentation(&mut self, name: &str, version: &str) {
        if !name.is_empty() {
            self.instrumentation
                .insert("instrumentation:name".to_string());
        }
        if !version.is_empty() {
            self.instrumentation
                .insert("instrumentation:version".to_string());
        }
    }
}

// Attr Value
/// A typed attribute value.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum AttrValue {
    Str(String),
    Int(i64),
    Float(#[serde(with = "float_text")] f64),
    Bool(bool),
    Array(Vec<AttrValue>),
    /// Exact `AnyValue` JSON, retained for trace retrieval and excluded from typed operands.
    Unsupported(String),
}

/// One named attribute and its typed value.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Attribute {
    pub key: String,
    pub value: AttrValue,
}

impl Attribute {
    #[must_use]
    pub fn new(key: impl Into<String>, value: AttrValue) -> Self {
        Self {
            key: key.into(),
            value,
        }
    }
}

pub(crate) fn normalized_attr_value(value: &AttrValue) -> Option<&AttrValue> {
    match value {
        AttrValue::Unsupported(_) => None,
        AttrValue::Array(values) if values.is_empty() => None,
        AttrValue::Array(values) if values.len() == 1 => values.first(),
        value => Some(value),
    }
}

impl AttrValue {
    /// The Arrow type of the column that holds this value.
    ///
    /// Strings, arrays and unsupported values all project as `Utf8`.
    #[must_use]
    pub fn arrow_data_type(&self) -> arrow::datatypes::DataType {
        use arrow::datatypes::DataType;
        match self {
            Self::Int(_) => DataType::Int64,
            Self::Float(_) => DataType::Float64,
            Self::Bool(_) => DataType::Boolean,
            Self::Str(_) | Self::Unsupported(_) | Self::Array(_) => DataType::Utf8,
        }
    }
}

// JSON round-tripping must retain NaN/infinities instead of converting to null.
mod float_text {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        value: impl std::borrow::Borrow<f64>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.borrow().to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<f64, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

// Typed Value
/// One typed tag value.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TypedValue {
    pub type_: String,
    pub value: String,
}

impl TypedValue {
    pub fn new(type_: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            type_: type_.into(),
            value: value.into(),
        }
    }
}

// Tag Scope
/// Tag discovery scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TagScope {
    Resource,
    Span,
    Intrinsic,
    Event,
    Link,
    Instrumentation,
}

/// A tag after removing its optional resource or span prefix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TagScopePrefix<'a> {
    pub tag: &'a str,
    pub scope: Option<TagScope>,
}

/// A normalized query tag and the attribute prefix it names, when present.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParsedTagScope<'a> {
    pub tag: &'a str,
    pub attribute: TagScopePrefix<'a>,
}

impl TagScope {
    /// Removes an optional leading dot and resolves instrumentation,
    /// resource, or span attribute prefixes.
    #[must_use]
    pub fn parse_attribute_tag(tag: &str) -> ParsedTagScope<'_> {
        let tag = tag.strip_prefix('.').unwrap_or(tag);
        let attribute = match tag.strip_prefix("instrumentation.") {
            Some(tag) => TagScopePrefix {
                tag,
                scope: Some(Self::Instrumentation),
            },
            None => Self::split_resource_or_span_prefix(tag),
        };
        ParsedTagScope { tag, attribute }
    }

    /// Splits a `resource.` or `span.` scope prefix off an attribute tag.
    ///
    /// Returns the bare attribute name, with the scope the prefix named, or
    /// the tag unchanged and no scope when it has neither prefix.
    #[must_use]
    pub fn split_resource_or_span_prefix(tag: &str) -> TagScopePrefix<'_> {
        if let Some(tag) = tag.strip_prefix("resource.") {
            TagScopePrefix {
                tag,
                scope: Some(Self::Resource),
            }
        } else if let Some(tag) = tag.strip_prefix("span.") {
            TagScopePrefix {
                tag,
                scope: Some(Self::Span),
            }
        } else {
            TagScopePrefix { tag, scope: None }
        }
    }
}

// Scoped Tag
/// Tag names grouped by scope.
#[derive(Clone, Debug, PartialEq)]
pub struct ScopedTag {
    pub scope: TagScope,
    pub tags: Vec<String>,
}

// Tag Name Groups
/// Tag names grouped by source scope before the requested scope is applied.
#[derive(Default)]
pub struct TagNameGroups {
    pub resource: BTreeSet<String>,
    pub span: BTreeSet<String>,
    pub event: BTreeSet<String>,
    pub link: BTreeSet<String>,
    pub instrumentation: BTreeSet<String>,
}

impl TagNameGroups {
    /// Returns nonempty groups allowed by `scope`, with intrinsic names
    /// included when the store has matching spans.
    #[must_use]
    pub fn into_scoped_tags(
        self,
        scope: Option<TagScope>,
        include_intrinsic: bool,
        intrinsic_tags: &[&str],
    ) -> Vec<ScopedTag> {
        let mut out = Vec::new();
        Self::push_group(&mut out, scope, TagScope::Resource, self.resource);
        Self::push_group(&mut out, scope, TagScope::Span, self.span);
        if include_intrinsic && scope.is_none_or(|scope| scope == TagScope::Intrinsic) {
            out.push(ScopedTag {
                scope: TagScope::Intrinsic,
                tags: intrinsic_tags
                    .iter()
                    .map(|tag| (*tag).to_string())
                    .collect(),
            });
        }
        Self::push_group(&mut out, scope, TagScope::Event, self.event);
        Self::push_group(&mut out, scope, TagScope::Link, self.link);
        Self::push_group(
            &mut out,
            scope,
            TagScope::Instrumentation,
            self.instrumentation,
        );
        out
    }

    fn push_group(
        out: &mut Vec<ScopedTag>,
        requested: Option<TagScope>,
        scope: TagScope,
        tags: BTreeSet<String>,
    ) {
        if requested.is_none_or(|requested| requested == scope) && !tags.is_empty() {
            out.push(ScopedTag {
                scope,
                tags: tags.into_iter().collect(),
            });
        }
    }
}

// Event Ref
/// One span event attached to a returned span.
#[derive(Clone, Debug, PartialEq)]
pub struct EventRef {
    /// How long after the span started the event fired.
    pub time_since_start: Time,
    pub name: String,
    pub attributes: Vec<Attribute>,
}

// Link Ref
/// One linked span reference attached to a returned span.
#[derive(Clone, Debug, PartialEq)]
pub struct LinkRef {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub attributes: Vec<Attribute>,
}

// Span Ref
/// One matched span in a result span set.
///
/// `Default` is a root span with zeroed ids and no attributes, events, or links.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SpanRef {
    pub span_id: [u8; 8],
    pub parent_span_id: Option<[u8; 8]>,
    pub name: String,
    pub kind: i32,
    pub nested_set_left: i32,
    pub nested_set_right: i32,
    pub nested_set_parent: i32,
    pub start_time_unix_nano: u64,
    /// How long the span ran.
    pub duration: Time,
    pub status_code: i32,
    pub status_message: String,
    pub instrumentation_name: String,
    pub instrumentation_version: String,
    pub resource_attributes: Vec<Attribute>,
    pub attributes: Vec<Attribute>,
    pub events: Vec<EventRef>,
    pub links: Vec<LinkRef>,
}

// Span Set
/// A matched span set.
#[derive(Clone, Debug, PartialEq)]
pub struct SpanSet {
    pub spans: Vec<SpanRef>,
    pub matched: u32,
    pub attributes: Vec<Attribute>,
}

// Trace Spans
/// Full span set for one trace.
#[derive(Clone, Debug, PartialEq)]
pub struct TraceSpans {
    pub trace_id: [u8; 16],
    pub root_service_name: String,
    pub root_trace_name: String,
    pub resource_attributes: Vec<Attribute>,
    pub spans: Vec<SpanRef>,
}

// Trace Result
/// One trace in a search response.
#[derive(Clone, Debug, PartialEq)]
pub struct TraceResult {
    pub trace_id: [u8; 16],
    pub root_service_name: String,
    pub root_trace_name: String,
    pub start_time_unix_nano: u64,
    /// How long the trace ran, from the earliest span start to the latest span
    /// end. The Tempo search JSON shows this duration twice, as `durationMs`
    /// and as the span-set `durationNanos`. Both come from this one field.
    pub duration: Time,
    pub span_sets: Vec<SpanSet>,
}

// Search Response
/// Search response.
#[derive(Clone, Debug, PartialEq)]
pub struct SearchResponse {
    pub traces: Vec<TraceResult>,
    pub inspected_traces: usize,
    /// Approximate span data the query inspected: the decoded size of the
    /// scanned cold and live batches, before filtering. The engine reports this
    /// value as the Tempo search `metrics.inspectedBytes`.
    pub inspected: ByteSize,
}

// Metric Label
/// One name and display value in a metrics series or exemplar.
#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct MetricLabel {
    pub key: String,
    pub value: String,
}

impl MetricLabel {
    #[must_use]
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

// Trace Metric Label Type
/// A metric label's type and OTLP `AnyValue` shape. String labels need no marker.
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, serde::Serialize, serde::Deserialize,
)]
pub enum TraceMetricLabelType {
    Int,
    Double,
    Bool,
    /// Canonical OTLP array value, preserving element order and types.
    Array,
    /// Pinned frontend-decoded nil, distinct from the string label `"nil"`.
    Nil,
}

// Trace Metric Exemplar
/// One Prometheus-style exemplar attached to a `TraceQL` metrics series.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TraceMetricExemplar {
    pub labels: Vec<MetricLabel>,
    /// Scalar types that cannot be recovered from a display label. Omitted
    /// entries are strings; e.g. integer 1 and string "1" remain distinct.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub label_types: std::collections::BTreeMap<String, TraceMetricLabelType>,
    pub value: f64,
    pub timestamp_ns: i64,
}

// Trace Metric Series
/// One timestamped sample in a metrics series.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MetricPoint {
    pub timestamp: i64,
    pub value: f64,
}

impl serde::Serialize for MetricPoint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeTuple as _;

        let mut point = serializer.serialize_tuple(2)?;
        point.serialize_element(&self.timestamp)?;
        point.serialize_element(&self.value)?;
        point.end()
    }
}

impl<'de> serde::Deserialize<'de> for MetricPoint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct MetricPointVisitor;

        impl<'de> serde::de::Visitor<'de> for MetricPointVisitor {
            type Value = MetricPoint;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a timestamp and metric value")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let timestamp = sequence
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let value = sequence
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;
                Ok(MetricPoint { timestamp, value })
            }
        }

        deserializer.deserialize_tuple(2, MetricPointVisitor)
    }
}

impl MetricPoint {
    #[must_use]
    pub const fn new(timestamp: i64, value: f64) -> Self {
        Self { timestamp, value }
    }
}

/// One `TraceQL` metrics series.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TraceMetricSeries {
    pub labels: Vec<MetricLabel>,
    /// Scalar types that cannot be recovered from a display label. Omitted
    /// entries are strings; e.g. integer 1 and string "1" remain distinct.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub label_types: std::collections::BTreeMap<String, TraceMetricLabelType>,
    pub points: Vec<MetricPoint>,
    pub exemplars: Vec<TraceMetricExemplar>,
}

// Trace Metrics Response
/// `TraceQL` metrics response.
#[derive(Clone, Debug, PartialEq)]
pub struct TraceMetricsResponse {
    pub series: Vec<TraceMetricSeries>,
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::{bytes, millis, nanos};

    use super::*;

    #[test]
    fn span_ref_holds_typed_attributes() {
        let s = SpanRef {
            span_id: [1; 8],
            parent_span_id: None,
            name: "op".into(),
            kind: 0,
            nested_set_left: 0,
            nested_set_right: 0,
            nested_set_parent: 0,
            start_time_unix_nano: 1000,
            duration: nanos(42),
            status_code: 0,
            status_message: String::new(),
            instrumentation_name: String::new(),
            instrumentation_version: String::new(),
            resource_attributes: Vec::new(),
            attributes: vec![
                Attribute::new("http.status_code", AttrValue::Int(200)),
                Attribute::new("ok", AttrValue::Bool(true)),
            ],
            events: Vec::new(),
            links: Vec::new(),
        };
        assert!(s.attributes[0].value == AttrValue::Int(200));
        assert!(s.attributes[1].value == AttrValue::Bool(true));
    }

    #[test]
    fn search_response_nests_span_sets() {
        let resp = SearchResponse {
            traces: vec![TraceResult {
                trace_id: [0xAB; 16],
                root_service_name: "checkout".into(),
                root_trace_name: "POST /pay".into(),
                start_time_unix_nano: 5,
                duration: millis(12),
                span_sets: vec![SpanSet {
                    spans: vec![],
                    matched: 3,
                    attributes: Vec::new(),
                }],
            }],
            inspected_traces: 1,
            inspected: bytes(4096),
        };
        assert!(resp.traces[0].span_sets[0].matched == 3);
        assert!(resp.traces[0].trace_id == [0xAB; 16]);
        assert!(resp.inspected_traces == 1);
        assert!(resp.inspected == bytes(4096));
    }

    #[test]
    fn tag_scope_is_copy() {
        let s = TagScope::Span;
        let c = s;
        assert!(s == TagScope::Span);
        assert!(c == TagScope::Span);
    }
}
