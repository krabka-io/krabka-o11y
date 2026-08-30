use super::{CompiledMatcher, MatchOp, SampleRow, label_value};

pub(crate) fn row_matches(row: &SampleRow, matchers: &[CompiledMatcher<'_>]) -> bool {
    matchers.iter().all(|matcher| match matcher {
        CompiledMatcher::Literal(matcher) => {
            let value = label_value(row, &matcher.name);
            match matcher.op {
                MatchOp::Eq => value.is_some_and(|value| value == matcher.value),
                MatchOp::Neq => value.is_none_or(|value| value != matcher.value),
                MatchOp::Re | MatchOp::Nre => unreachable!("regex matchers are compiled"),
            }
        }
        CompiledMatcher::Regex(matcher, regex) => {
            let regex_matched =
                label_value(row, &matcher.name).is_some_and(|value| regex.is_match(value));
            match matcher.op {
                MatchOp::Re => regex_matched,
                MatchOp::Nre => !regex_matched,
                MatchOp::Eq | MatchOp::Neq => unreachable!("literal matchers are not compiled"),
            }
        }
    })
}
