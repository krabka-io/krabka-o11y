use super::{ComparisonOp, Field, ScalarExpr, Value};

#[derive(Clone, Debug, PartialEq)]
/// A Boolean expression over trace fields and scalar values.
pub enum FieldExpr {
    Comparison {
        lhs: Field,
        op: ComparisonOp,
        rhs: Value,
    },
    /// A comparison whose operands are resolved independently on each span.
    FieldComparison {
        lhs: Field,
        op: ComparisonOp,
        rhs: Field,
    },
    /// Scalar arithmetic comparison evaluated without SQL value coercion.
    ExpressionComparison {
        lhs: ScalarExpr,
        op: ComparisonOp,
        rhs: ScalarExpr,
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

impl FieldExpr {
    pub(crate) fn has_field_comparison(&self) -> bool {
        match self {
            Self::FieldComparison { .. } | Self::ExpressionComparison { .. } => true,
            Self::And(lhs, rhs) | Self::Or(lhs, rhs) => {
                lhs.has_field_comparison() || rhs.has_field_comparison()
            }
            Self::Not(inner) => inner.has_field_comparison(),
            Self::Comparison { .. } | Self::Field(_) | Self::Const(_) => false,
        }
    }
}
