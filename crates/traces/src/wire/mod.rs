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

/// Ingest-edge wire error.
pub type WireError = TracesError;

/// Which push door a request arrived on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireFormat {
    Otlp,
    Zipkin,
    Jaeger,
}

/// Pick the decoder from the request path.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub fn negotiate(path: &str, content_type: Option<&str>) -> Result<WireFormat, WireError> {
    match path {
        "/v1/traces" | "/api/push" => Ok(WireFormat::Otlp),
        "/api/v2/spans" => Ok(WireFormat::Zipkin),
        "/api/traces" => Ok(WireFormat::Jaeger),
        other => Err(WireError::UnsupportedContentType(format!(
            "{other} (content-type {})",
            content_type.unwrap_or("none")
        ))),
    }
}
