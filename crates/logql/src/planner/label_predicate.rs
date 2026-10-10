use super::{BlockStoreError, LabelMatcher, LabelPredicate};

pub(crate) fn label_predicate(matcher: &LabelMatcher) -> Result<LabelPredicate, BlockStoreError> {
    LabelPredicate::new(matcher.name.clone(), matcher.op, matcher.value.clone())
}
