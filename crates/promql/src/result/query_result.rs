use super::*;

/// A `PromQL` evaluation result.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum QueryResult {
    Scalar { ts_ms: i64, value: f64 },
    InstantVector(Vec<InstantSample>),
    RangeMatrix(Vec<RangeSeries>),
    Str { ts_ms: i64, value: String },
}

impl QueryResult {
    /// Prometheus `data.resultType` string.
    #[must_use]
    pub fn result_type(&self) -> &'static str {
        match self {
            Self::Scalar { .. } => "scalar",
            Self::InstantVector(_) => "vector",
            Self::RangeMatrix(_) => "matrix",
            Self::Str { .. } => "string",
        }
    }
}
