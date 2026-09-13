#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// A TraceQL scalar or field comparison operator.
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
