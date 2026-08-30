use super::*;

pub(crate) fn label_predicate(matcher: &LabelMatcher) -> Result<LabelPredicate, BlockStoreError> {
    LabelPredicate::new(
        matcher.name.clone(),
        match matcher.op {
            MatchOp::Equal => BlockMatchOp::Equal,
            MatchOp::NotEqual => BlockMatchOp::NotEqual,
            MatchOp::RegexEqual => BlockMatchOp::RegexEqual,
            MatchOp::RegexNotEqual => BlockMatchOp::RegexNotEqual,
        },
        matcher.value.clone(),
    )
}
