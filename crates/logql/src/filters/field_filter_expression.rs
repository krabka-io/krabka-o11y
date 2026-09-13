use super::{FieldFilter, FieldFilterLogicOp, Labels};

#[derive(Clone, Debug, PartialEq)]
/// A grouped Boolean expression over extracted-field filters.
pub enum FieldFilterExpression {
    Filter(FieldFilter),
    Group(Box<FieldFilterExpression>),
    Chain {
        first: Box<FieldFilterExpression>,
        rest: Vec<(FieldFilterLogicOp, FieldFilterExpression)>,
    },
}

impl FieldFilterExpression {
    #[must_use]
    /// Applies the expression and records conversion errors in the extracted fields.
    pub fn apply(&self, fields: &mut Labels) -> bool {
        match self {
            Self::Filter(filter) => filter.apply(fields),
            Self::Group(expression) => expression.apply(fields),
            Self::Chain { first, rest } => {
                let mut result = first.apply(fields);
                for (op, expression) in rest {
                    match op {
                        FieldFilterLogicOp::And => result = result && expression.apply(fields),
                        FieldFilterLogicOp::Or => result = result || expression.apply(fields),
                    }
                }
                result
            }
        }
    }

    #[must_use]
    /// Tests the expression without changing the supplied fields.
    pub fn matches(&self, fields: &Labels) -> bool {
        let mut fields = fields.clone();
        self.apply(&mut fields)
    }
}
