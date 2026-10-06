use super::{Field, FieldExpr, Value};

/// Arithmetic operation evaluated on the original typed span values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArithmeticOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
}

/// A scalar expression; field references retain their scope and runtime type.
#[derive(Clone, Debug, PartialEq)]
pub enum ScalarExpr {
    Field(Field),
    Literal(Value),
    Predicate(Box<FieldExpr>),
    Negate(Box<ScalarExpr>),
    Binary {
        lhs: Box<ScalarExpr>,
        op: ArithmeticOp,
        rhs: Box<ScalarExpr>,
    },
}

impl ScalarExpr {
    pub(crate) fn collect_fields<'a>(&'a self, out: &mut Vec<&'a Field>) {
        match self {
            Self::Field(field) => out.push(field),
            Self::Predicate(expr) => expr.collect_fields(out),
            Self::Binary { lhs, rhs, .. } => {
                lhs.collect_fields(out);
                rhs.collect_fields(out);
            }
            Self::Negate(inner) => inner.collect_fields(out),
            Self::Literal(_) => {}
        }
    }
}
