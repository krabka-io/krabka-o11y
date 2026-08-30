use super::*;

#[derive(Clone, Debug, PartialEq)]
pub enum FieldExpr {
    Comparison {
        lhs: Field,
        op: ComparisonOp,
        rhs: Value,
    },
    And(Box<FieldExpr>, Box<FieldExpr>),
    Or(Box<FieldExpr>, Box<FieldExpr>),
    Not(Box<FieldExpr>),
    Field(Field),
    /// A constant boolean filter.
    ///
    /// The empty spanset `{}` and the scalar-boolean spanset `{ true }` lower
    /// to `Const(true)`, which matches every span. The spanset `{ false }`
    /// lowers to `Const(false)`, which matches no span. This mirrors Grafana
    /// Tempo, whose Explore "Search" tab and TraceQL-metrics default to `{}`.
    Const(bool),
}
