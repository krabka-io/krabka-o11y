#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// A `TraceQL` scalar or field comparison operator.
pub enum ComparisonOp {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    Re,
    Nre,
}

/// A numeric comparison against a constant threshold.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct NumericFilter {
    pub(crate) op: ComparisonOp,
    pub(crate) value: f64,
}
