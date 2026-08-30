//! Push-door wire surfaces for trace ingest.

pub mod jaeger;
pub mod jaeger_grpc;
pub mod otlp;
pub mod zipkin;

use crate::error::TracesError;

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn negotiate_trace_push_paths() {
        for (path, content_type, want) in [
            (
                "/v1/traces",
                Some("application/x-protobuf"),
                WireFormat::Otlp,
            ),
            ("/api/push", None, WireFormat::Otlp),
            (
                "/api/v2/spans",
                Some("application/json"),
                WireFormat::Zipkin,
            ),
            ("/api/traces", None, WireFormat::Jaeger),
        ] {
            assert2::assert!(negotiate(path, content_type).unwrap() == want);
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
