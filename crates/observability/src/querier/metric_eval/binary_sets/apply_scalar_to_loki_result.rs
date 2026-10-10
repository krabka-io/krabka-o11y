use super::{HttpQueryError, MetricValue, ParseError, Value, parse_metric_sample_value};

/// The scalar literal operand of a binary operator in `query`.
#[derive(Clone, Copy)]
pub(crate) struct ScalarLiteral<'a> {
    pub(crate) text: &'a str,
    pub(crate) query: &'a str,
}

impl ScalarLiteral<'_> {
    /// The literal's value. An unparsable literal is a parse error of the
    /// query.
    pub(crate) fn parse(self) -> Result<MetricValue, HttpQueryError> {
        parse_metric_sample_value(self.text).ok_or_else(|| HttpQueryError::LokiParse {
            query: self.query.to_string(),
            source: ParseError::Syntax {
                message: "expected scalar literal".to_string(),
                position: 0,
            },
        })
    }
}

/// Keeps each series of a Loki result that `apply_series` accepts with
/// `scalar` as its operand.
pub(crate) fn apply_scalar_to_loki_result(
    loki_result: &mut Value,
    scalar: MetricValue,
    apply_series: impl Fn(&mut Value, MetricValue) -> bool,
) {
    let Some(results) = loki_result
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };

    results.retain_mut(|series| apply_series(series, scalar));
}
