use super::{Time, LoadSeries, ExpectLine, AnnotationExpect, RangeExpect};

/// A top-level Prometheus `.test` statement.
#[derive(Clone, Debug, PartialEq)]
pub enum Statement {
    /// Loads one or more series at a fixed step.
    Load {
        /// Step between loaded samples.
        step: Time,
        /// Series loaded by this statement.
        series: Vec<LoadSeries>,
    },
    /// Evaluates an instant query.
    EvalInstant {
        /// Evaluation timestamp in milliseconds.
        at_ms: i64,
        /// `PromQL` expression.
        expr: String,
        /// Expected output lines.
        expect: Vec<ExpectLine>,
        /// Expected annotation directives: `warn`, `info`, `no_warn`, and `no_info`.
        annotations: Vec<AnnotationExpect>,
        /// Optional matrix expectation metadata for instant range-vector results.
        range_expect: Option<RangeExpect>,
        /// Expected failure message. An empty message matches any failure.
        fail_message: Option<String>,
    },
    /// Evaluates a range query.
    EvalRange {
        /// Range start timestamp in milliseconds.
        start_ms: i64,
        /// Range end timestamp in milliseconds.
        end_ms: i64,
        /// Query step.
        step: Time,
        /// `PromQL` expression.
        expr: String,
        /// Expected output lines.
        expect: Vec<ExpectLine>,
        /// Expected annotation directives: `warn`, `info`, `no_warn`, and `no_info`.
        annotations: Vec<AnnotationExpect>,
        /// Expected failure message. An empty message matches any failure.
        fail_message: Option<String>,
    },
    /// Clears the loaded series.
    Clear,
}
