use super::*;

/// The low 64 bits of a 128-bit Jaeger trace id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct TraceIdLow(pub i64);
