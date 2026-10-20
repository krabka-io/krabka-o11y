use super::{FieldFilter, FieldFilterLogicOp, FieldFilterStep, Labels};

#[derive(Clone, Debug, PartialEq)]
/// A grouped Boolean expression over extracted-field filters.
pub enum FieldFilterExpression {
    Filter(FieldFilter),
    Group(Box<FieldFilterExpression>),
    Chain {
        first: Box<FieldFilterExpression>,
        rest: Vec<FieldFilterStep<FieldFilterExpression>>,
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
                for step in rest {
                    match step.operation {
                        FieldFilterLogicOp::And => result = result && step.filter.apply(fields),
                        FieldFilterLogicOp::Or => result = result || step.filter.apply(fields),
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
