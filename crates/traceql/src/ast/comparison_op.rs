#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
