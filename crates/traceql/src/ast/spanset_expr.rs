use super::{FieldExpr, StructuralOp};

#[derive(Clone, Debug, PartialEq)]
/// A `TraceQL` expression that selects or relates spansets.
pub enum SpansetExpr {
    Selector(Box<FieldExpr>),
    And(Box<SpansetExpr>, Box<SpansetExpr>),
    Or(Box<SpansetExpr>, Box<SpansetExpr>),
    Structural {
        op: StructuralOp,
        lhs: Box<SpansetExpr>,
        rhs: Box<SpansetExpr>,
    },
}
