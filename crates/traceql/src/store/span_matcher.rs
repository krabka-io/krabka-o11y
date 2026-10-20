use std::{borrow::Cow, collections::HashMap, sync::Arc};

use super::{MatchCmp, MatchScope, MatchValue, PreparedRegex};

#[derive(Clone, Debug, PartialEq)]
pub struct SpanMatcher {
    pub scope: MatchScope,
    pub key: String,
    pub op: MatchCmp,
    pub value: MatchValue,
    pub negated: bool,
}

impl SpanMatcher {
    /// Prepares each distinct string regex once for the scan, including invalid patterns.
    ///
    /// Clones share the compiled result. Matchers without regex operands remain unchanged.
    #[must_use]
    pub fn prepare_regexes(matchers: &[Self]) -> Cow<'_, [Self]> {
        if !matchers.iter().any(|matcher| {
            matches!(matcher.op, MatchCmp::Re | MatchCmp::Nre)
                && matches!(matcher.value, MatchValue::Str(_))
        }) {
            return Cow::Borrowed(matchers);
        }
        let mut regexes = HashMap::new();
        let mut prepared = matchers.to_vec();
        for matcher in &mut prepared {
            if matches!(matcher.op, MatchCmp::Re | MatchCmp::Nre)
                && let MatchValue::Str(pattern) = &matcher.value
            {
                let regex = regexes
                    .entry(pattern.clone())
                    .or_insert_with(|| Arc::new(PreparedRegex::new(pattern)));
                matcher.value = MatchValue::Regex(Arc::clone(regex));
            }
        }
        Cow::Owned(prepared)
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn preparation_shares_successes_and_failures() {
        for pattern in ["GET.*", "["] {
            let matchers = [MatchCmp::Re, MatchCmp::Nre].map(|op| SpanMatcher {
                scope: MatchScope::Span,
                key: "method".into(),
                op,
                value: MatchValue::Str(pattern.into()),
                negated: false,
            });
            let prepared = SpanMatcher::prepare_regexes(&matchers);
            let (MatchValue::Regex(first), MatchValue::Regex(second)) =
                (&prepared[0].value, &prepared[1].value)
            else {
                panic!("regex operands should be prepared");
            };
            assert!(Arc::ptr_eq(first, second));
            assert!(first.compiled.is_some() == (pattern != "["));
            let cloned = prepared[0].clone();
            let MatchValue::Regex(cloned) = cloned.value else {
                panic!("a clone should retain the prepared regex");
            };
            assert!(Arc::ptr_eq(first, &cloned));
            assert!(matches!(
                SpanMatcher::prepare_regexes(&prepared),
                Cow::Borrowed(_)
            ));
        }
    }
}
