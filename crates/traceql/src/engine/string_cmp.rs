use super::*;

pub(crate) fn string_cmp(value: &str, op: ComparisonOp, rhs: &str, regexes: &CompareRegexCache) -> bool {
    match op {
        ComparisonOp::Eq => value == rhs,
        ComparisonOp::Neq => value != rhs,
        // The pattern was precompiled once per query (see CompareRegexCache); an
        // uncompilable pattern is absent from the cache and yields non-match.
        ComparisonOp::Re => regexes.get(rhs).is_some_and(|re| re.is_match(value)),
        ComparisonOp::Nre => regexes.get(rhs).is_some_and(|re| !re.is_match(value)),
        ComparisonOp::Lt | ComparisonOp::Lte | ComparisonOp::Gt | ComparisonOp::Gte => false,
    }
}
