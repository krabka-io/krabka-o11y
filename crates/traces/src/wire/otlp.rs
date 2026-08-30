//! OTLP `TracesData` to internal spans.

use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, KeyValue as OtlpKv, any_value::Value},
    trace::v1::{Status, TracesData, span::SpanKind as OtlpKind},
};

use super::WireError;
use crate::span::{AttrValue, EventRecord, KeyValue, LinkRecord, Span, SpanKind, StatusCode};

#[cfg(test)]
mod tests {

    use opentelemetry_proto::tonic::{
        common::v1::{
            AnyValue, ArrayValue, InstrumentationScope, KeyValue as OtlpKv, any_value::Value,
        },
        resource::v1::Resource,
        trace::v1::{
            ResourceSpans, ScopeSpans, Span as OtlpSpan, Status, TracesData,
            span::SpanKind as OtlpKind,
        },
    };

    /// `any_to_text` renders every scalar OTLP value as a string. A collapsed
    /// body returns None, an empty string, or a fixed word, so each arm is
    /// pinned to a rendering that is none of those -- and both booleans are
    /// checked, since one of them renders as a word a mutant might guess.
    #[test]
    fn an_otlp_any_value_renders_as_text_for_every_scalar_kind() {
        let text = |value: Value| super::any_to_text(&AnyValue { value: Some(value) });

        assert2::check!(text(Value::StringValue("hi".into())) == Some("hi".to_string()));
        assert2::check!(text(Value::IntValue(7)) == Some("7".to_string()));
        assert2::check!(text(Value::DoubleValue(1.5)) == Some("1.5".to_string()));
        assert2::check!(text(Value::BoolValue(true)) == Some("true".to_string()));
        assert2::check!(text(Value::BoolValue(false)) == Some("false".to_string()));
        assert2::check!(text(Value::BytesValue(vec![0xAB, 0xCD])) == Some("abcd".to_string()));

        // An array has no text form, and neither has an absent value. Both
        // must be None rather than an empty string, which would render as a
        // present-but-blank attribute downstream.
        assert2::check!(
            text(Value::ArrayValue(ArrayValue { values: vec![] })).is_none(),
            "an array is not text"
        );
        assert2::check!(super::any_to_text(&AnyValue { value: None }).is_none());
    }

    use super::*;

    fn kv(key: &str, value: &str) -> OtlpKv {
        OtlpKv {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(value.into())),
            }),
            ..OtlpKv::default()
        }
    }

    fn data() -> TracesData {
        let otlp_span = OtlpSpan {
            trace_id: vec![1; 16],
            span_id: vec![2; 8],
            parent_span_id: Vec::new(),
            name: "GET /".into(),
            kind: OtlpKind::Server as i32,
            start_time_unix_nano: 1_000,
            end_time_unix_nano: 1_500,
            attributes: vec![kv("http.method", "GET")],
            status: Some(Status {
                code: 1,
                message: String::new(),
            }),
            ..OtlpSpan::default()
        };

        TracesData {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![kv("service.name", "api")],
                    ..Resource::default()
                }),
                scope_spans: vec![ScopeSpans {
                    spans: vec![otlp_span],
                    ..ScopeSpans::default()
                }],
                ..ResourceSpans::default()
            }],
        }
    }

    #[test]
    fn decodes_one_span_with_resource_attrs() {
        let spans = decode_otlp(&data()).unwrap();
        assert2::assert!(
            spans
                == vec![Span {
                    trace_id: [1; 16],
                    span_id: [2; 8],
                    parent_span_id: None,
                    name: "GET /".into(),
                    kind: SpanKind::Server,
                    start_ns: 1_000,
                    duration_ns: 500,
                    status: StatusCode::Ok,
                    status_message: String::new(),
                    resource_attrs: vec![KeyValue {
                        key: "service.name".into(),
                        value: AttrValue::Str("api".into()),
                    }],
                    span_attrs: vec![KeyValue {
                        key: "http.method".into(),
                        value: AttrValue::Str("GET".into()),
                    }],
                    events: Vec::new(),
                    links: Vec::new(),
                    instrumentation_scope: String::new(),
                    instrumentation_version: String::new(),
                }]
        );
    }

    #[test]
    fn decodes_array_attributes_as_repeated_values() {
        let mut data = data();
        data.resource_spans[0].scope_spans[0].spans[0]
            .attributes
            .push(OtlpKv {
                key: "http.method".into(),
                value: Some(AnyValue {
                    value: Some(Value::ArrayValue(ArrayValue {
                        values: vec![
                            AnyValue {
                                value: Some(Value::StringValue("GET".into())),
                            },
                            AnyValue {
                                value: Some(Value::StringValue("POST".into())),
                            },
                        ],
                    })),
                }),
                ..OtlpKv::default()
            });

        let spans = decode_otlp(&data).unwrap();
        let methods = spans[0]
            .span_attrs
            .iter()
            .filter(|attr| attr.key == "http.method")
            .map(|attr| &attr.value)
            .collect::<Vec<_>>();

        assert2::assert!(methods.contains(&&AttrValue::Str("GET".into())));
        assert2::assert!(methods.contains(&&AttrValue::Str("POST".into())));
    }

    #[test]
    fn decodes_instrumentation_scope_version() {
        let mut data = data();
        data.resource_spans[0].scope_spans[0].scope = Some(InstrumentationScope {
            name: "tracer".into(),
            version: "1.2.3".into(),
            attributes: vec![OtlpKv {
                key: "library.language".into(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue("rust".into())),
                }),
                ..OtlpKv::default()
            }],
            ..InstrumentationScope::default()
        });

        let spans = decode_otlp(&data).unwrap();

        assert2::assert!(
            (
                spans[0].instrumentation_scope.as_str(),
                spans[0].instrumentation_version.as_str(),
            ) == ("tracer", "1.2.3")
        );
        assert2::assert!(spans[0].span_attrs.iter().any(|attribute| {
            attribute.key == "__instrumentation.library.language"
                && attribute.value == AttrValue::Str("rust".into())
        }));
    }

    #[test]
    fn rejects_wrong_length_trace_id() {
        let mut data = data();
        data.resource_spans[0].scope_spans[0].spans[0].trace_id = vec![1; 8];
        assert2::assert!(decode_otlp(&data).is_err());
    }
}

// === split-modules: generated submodules ===
mod any_to_attr;
mod any_to_text;
mod decode_otlp;
mod fixed16;
mod fixed8;
mod kind_of;
mod kv_to_attrs;
mod kvs;
mod status_of;

use any_to_attr::any_to_attr;
use any_to_text::any_to_text;
pub use decode_otlp::decode_otlp;
use fixed8::fixed8;
use fixed16::fixed16;
use kind_of::kind_of;
use kv_to_attrs::kv_to_attrs;
use kvs::kvs;
use status_of::status_of;
