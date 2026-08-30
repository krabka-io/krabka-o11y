use super::*;

/// Exemplar decoded from a `remote_write` request.
#[derive(Clone, Debug, PartialEq)]
pub struct DecodedExemplar {
    pub labels: Labels,
    pub timestamp_ms: i64,
    pub value: f64,
}
