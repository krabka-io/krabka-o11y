use super::{CompiledMatcher, LabelMatcher, MatchOp, ProfileError, Regex};

pub(crate) fn compile_matchers(
    matchers: &[LabelMatcher],
) -> Result<Vec<CompiledMatcher<'_>>, ProfileError> {
    matchers
        .iter()
        .map(|matcher| match matcher.op {
            MatchOp::Eq | MatchOp::Neq => Ok(CompiledMatcher::Literal(matcher)),
            MatchOp::Re | MatchOp::Nre => Regex::new(&format!("^(?:{})$", matcher.value))
                .map(|regex| CompiledMatcher::Regex(matcher, regex))
                .map_err(|err| ProfileError::Plan(format!("bad matcher regex: {err}"))),
        })
        .collect()
}
