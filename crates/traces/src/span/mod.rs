//! Internal span model every push door lowers into before WAL encoding.

use serde::{Deserialize, Serialize};

pub mod batch;
pub mod nested_set;

#[cfg(test)]
mod tests {

    use super::*;

    fn span(parent: Option<[u8; 8]>) -> Span {
        Span {
            trace_id: [1; 16],
            span_id: [2; 8],
            parent_span_id: parent,
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
                key: "http.status_code".into(),
                value: AttrValue::Int(200),
            }],
            events: Vec::new(),
            links: Vec::new(),
            instrumentation_scope: "tracer".into(),
            instrumentation_version: "1.2.3".into(),
        }
    }

    #[test]
    fn root_detection() {
        assert2::assert!(span(None).is_root());
        assert2::assert!(!span(Some([3; 8])).is_root());
    }

    #[test]
    fn kind_round_trips_i32() {
        for kind in [
            SpanKind::Unspecified,
            SpanKind::Internal,
            SpanKind::Server,
            SpanKind::Client,
            SpanKind::Producer,
            SpanKind::Consumer,
        ] {
            assert2::assert!(SpanKind::from_i32(kind.as_i32()) == kind);
        }
    }

    #[test]
    fn status_round_trips_i32() {
        for status in [StatusCode::Unset, StatusCode::Ok, StatusCode::Error] {
            assert2::assert!(StatusCode::from_i32(status.as_i32()) == status);
        }
    }
}

// === split-modules: generated submodules ===
mod attr_value;
mod event_record;
mod key_value;
mod link_record;
mod span;
mod span_kind;
mod status_code;

pub use attr_value::AttrValue;
pub use event_record::EventRecord;
pub use key_value::KeyValue;
pub use link_record::LinkRecord;
pub use span::Span;
pub use span_kind::SpanKind;
pub use status_code::StatusCode;
