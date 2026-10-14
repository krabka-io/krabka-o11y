//! Push-door wire surfaces for trace ingest.

pub mod jaeger;
pub mod jaeger_grpc;
pub mod otlp;
pub mod zipkin;

use crate::error::TracesError;

#[cfg(test)]
mod tests {

    use super::*;

    struct PushPathCase {
        path: &'static str,
        content_type: Option<&'static str>,
        expected_format: WireFormat,
    }

    #[test]
    fn negotiate_trace_push_paths() {
        for case in [
            PushPathCase {
                path: "/v1/traces",
                content_type: Some("application/x-protobuf"),
                expected_format: WireFormat::Otlp,
            },
            PushPathCase {
                path: "/api/push",
                content_type: None,
                expected_format: WireFormat::Otlp,
            },
            PushPathCase {
                path: "/api/v2/spans",
                content_type: Some("application/json"),
                expected_format: WireFormat::Zipkin,
            },
            PushPathCase {
                path: "/api/traces",
                content_type: None,
                expected_format: WireFormat::Jaeger,
            },
        ] {
            assert2::assert!(
                negotiate(case.path, case.content_type).unwrap() == case.expected_format
            );
        }
    }

    #[test]
    fn negotiate_unknown_path_is_415() {
        let err = negotiate("/nope", Some("text/plain")).unwrap_err();
        assert2::assert!(err.status_code() == 415);
    }
}

mod negotiate;
mod wire_error;
mod wire_format;

pub use negotiate::negotiate;
pub use wire_error::WireError;
pub use wire_format::WireFormat;
