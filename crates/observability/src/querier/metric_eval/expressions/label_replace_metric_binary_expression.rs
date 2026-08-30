use super::*;

pub(crate) enum LabelReplaceMetricBinaryExpression {
    Arithmetic {
        left: String,
        op: MetricScalarArithmeticOp,
        matching: Option<MetricVectorMatching>,
        right: String,
    },
    Comparison {
        left: String,
        op: ComparisonOp,
        bool_modifier: bool,
        matching: Option<MetricVectorMatching>,
        right: String,
    },
    Set {
        left: String,
        op: MetricBinarySetOp,
        matching: Option<MetricVectorMatching>,
        right: String,
    },
}
