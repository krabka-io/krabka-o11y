//! OTLP `TracesData` to internal spans.

use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, KeyValue as OtlpKv},
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
    use crate::span::test_span::{api_server_span, string_attr};

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
                    duration_ns: 500,
                    span_attrs: vec![string_attr("http.method", "GET")],
                    ..api_server_span()
                }]
        );
    }

    #[test]
    fn decodes_array_attributes_without_losing_array_identity() {
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

        assert2::assert!(
            methods
                == vec![
                    &AttrValue::Str("GET".into()),
                    &AttrValue::Array(vec![
                        AttrValue::Str("GET".into()),
                        AttrValue::Str("POST".into())
                    ]),
                ]
        );
    }

    #[test]
    fn array_wire_identity_survives_empty_singleton_and_nested_values() {
        let mut data = data();
        let values = [
            AttrValue::Array(Vec::new()),
            AttrValue::Array(vec![AttrValue::Int(7)]),
            AttrValue::Array(vec![AttrValue::Double(1.25), AttrValue::Double(2.5)]),
            AttrValue::Array(vec![AttrValue::Bool(true), AttrValue::Bool(false)]),
            AttrValue::Array(vec![AttrValue::Str("one".into()), AttrValue::Int(7)]),
            AttrValue::Array(vec![AttrValue::Array(vec![AttrValue::Bytes(vec![
                0xff, 0x00,
            ])])]),
        ];
        for (index, value) in values.iter().enumerate() {
            data.resource_spans[0].scope_spans[0].spans[0]
                .attributes
                .push(OtlpKv {
                    key: format!("array{index}"),
                    value: Some(value.otlp_value()),
                    ..OtlpKv::default()
                });
        }
        let spans = decode_otlp(&data).unwrap();
        for (index, expected) in values.iter().enumerate() {
            let actual = &spans[0]
                .span_attrs
                .iter()
                .find(|attr| attr.key == format!("array{index}"))
                .unwrap()
                .value;
            assert2::check!(actual == expected);
            assert2::check!(actual.otlp_value() == expected.otlp_value());
        }
    }

    #[test]
    fn empty_wire_values_survive_ingest_inside_arrays() {
        let mut data = data();
        let expected = AnyValue {
            value: Some(Value::ArrayValue(ArrayValue {
                values: vec![
                    AnyValue { value: None },
                    AnyValue {
                        value: Some(Value::IntValue(7)),
                    },
                    AnyValue { value: None },
                ],
            })),
        };
        data.resource_spans[0].scope_spans[0].spans[0].attributes = vec![OtlpKv {
            key: "empty-elements".into(),
            value: Some(expected.clone()),
            ..OtlpKv::default()
        }];
        let spans = decode_otlp(&data).unwrap();
        let attr = spans[0]
            .span_attrs
            .iter()
            .find(|attr| attr.key == "empty-elements")
            .unwrap();
        assert2::check!(attr.value.otlp_value() == expected);
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

mod any_to_attr;
#[cfg(test)]
mod any_to_text;
mod decode_otlp;
mod fixed16;
mod fixed8;
mod kind_of;
mod kv_to_attrs;
mod kvs;
mod status_of;

use any_to_attr::any_to_attr;
#[cfg(test)]
use any_to_text::any_to_text;
pub use decode_otlp::decode_otlp;
use fixed8::fixed8;
use fixed16::fixed16;
use kind_of::kind_of;
use kv_to_attrs::kv_to_attrs;
use kvs::kvs;
use status_of::status_of;
