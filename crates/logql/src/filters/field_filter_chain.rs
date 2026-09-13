use super::{FieldFilter, FieldFilterLogicOp, Labels};

#[derive(Clone, Debug, PartialEq)]
/// A left-to-right chain of field filters and Boolean operators.
pub struct FieldFilterChain {
    pub(crate) first: FieldFilter,
    pub(crate) rest: Vec<(FieldFilterLogicOp, FieldFilter)>,
}

impl FieldFilterChain {
    /// Creates a filter chain from its first filter and remaining operations.
    #[must_use]
    /// Tests the chain without changing the supplied fields.
    pub fn new(first: FieldFilter, rest: Vec<(FieldFilterLogicOp, FieldFilter)>) -> Self {
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
        for (op, filter) in &self.rest {
            match op {
                FieldFilterLogicOp::And => result = result && filter.apply(fields),
                FieldFilterLogicOp::Or => result = result || filter.apply(fields),
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
    pub fn rest(&self) -> &[(FieldFilterLogicOp, FieldFilter)] {
        &self.rest
    }
}
