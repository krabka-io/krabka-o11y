use super::*;

#[derive(Clone, Debug, PartialEq)]
pub struct FieldFilterChain {
    pub(crate) first: FieldFilter,
    pub(crate) rest: Vec<(FieldFilterLogicOp, FieldFilter)>,
}

impl FieldFilterChain {
    #[must_use]
    pub fn new(first: FieldFilter, rest: Vec<(FieldFilterLogicOp, FieldFilter)>) -> Self {
        Self { first, rest }
    }

    #[must_use]
    pub fn matches(&self, fields: &Labels) -> bool {
        let mut fields = fields.clone();
        self.apply(&mut fields)
    }

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
    pub fn first(&self) -> &FieldFilter {
        &self.first
    }

    #[must_use]
    pub fn rest(&self) -> &[(FieldFilterLogicOp, FieldFilter)] {
        &self.rest
    }
}
