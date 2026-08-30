use super::*;

/// Whether `matcher` matches a series for which the label is absent, that is
/// whether the matcher matches the empty string. This follows Prometheus
/// `Matcher.Matches("")` semantics. A selector built entirely from such
/// matchers restricts nothing.
pub(crate) fn matcher_matches_empty(matcher: &LabelMatcher) -> Result<bool> {
    match matcher.op {
        MatchOp::Eq => Ok(matcher.value.is_empty()),
        MatchOp::Neq => Ok(!matcher.value.is_empty()),
        MatchOp::Re | MatchOp::Nre => {
            let regex = regex::Regex::new(&anchored_regex(&matcher.value)).map_err(|error| {
                BlockStoreError::InvalidBlock(format!("invalid label matcher regex: {error}"))
            })?;
            let regex_matches_empty = regex.is_match("");
            Ok(match matcher.op {
                MatchOp::Re => regex_matches_empty,
                // `name!~"re"` selects series whose value does not match `re`;
                // the empty/absent value is selected exactly when `re` itself
                // does not match the empty string.
                _ => !regex_matches_empty,
            })
        }
    }
}
