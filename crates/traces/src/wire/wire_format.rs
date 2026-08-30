use super::*;

/// Which push door a request arrived on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireFormat {
    Otlp,
    Zipkin,
    Jaeger,
}
