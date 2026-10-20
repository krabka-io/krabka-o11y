use super::{FieldFilter, FieldFilterLogicOp, Labels};

#[derive(Clone, Debug, PartialEq)]
pub struct FieldFilterStep<T> {
    pub operation: FieldFilterLogicOp,
    pub filter: T,
}

#[derive(Clone, Debug, PartialEq)]
/// A left-to-right chain of field filters and Boolean operators.
pub struct FieldFilterChain {
    pub(crate) first: FieldFilter,
    pub(crate) rest: Vec<FieldFilterStep<FieldFilter>>,
}

impl FieldFilterChain {
    /// Creates a filter chain from its first filter and remaining operations.
    #[must_use]
    /// Tests the chain without changing the supplied fields.
    pub fn new(first: FieldFilter, rest: Vec<FieldFilterStep<FieldFilter>>) -> Self {
        Self { first, rest }
    }

    #[must_use]
    pub fn matches(&self, fields: &Labels) -> bool {
        let mut fields = fields.clone();
        self.apply(&mut fields)
    }

    /// Applies the chain and records conversion errors in the extracted fields.
    pub fn apply(&self, fields: &mut Labels) -> bool {
        let mut result = self.first.apply(fields);
        for step in &self.rest {
            match step.operation {
                FieldFilterLogicOp::And => result = result && step.filter.apply(fields),
                FieldFilterLogicOp::Or => result = result || step.filter.apply(fields),
            }
        }
        result
    }

    #[must_use]
    /// Returns the first filter.
    pub fn first(&self) -> &FieldFilter {
        &self.first
    }

    #[must_use]
    /// Returns the remaining operator and filter pairs.
    pub fn rest(&self) -> &[FieldFilterStep<FieldFilter>] {
        &self.rest
    }
}
