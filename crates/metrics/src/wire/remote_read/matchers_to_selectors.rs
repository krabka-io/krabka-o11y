use super::{LabelMatcher, MatchOp, RemoteReadError, v1};

/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn matchers_to_selectors(
    query: &v1::Query,
) -> Result<(Vec<LabelMatcher>, i64, i64), RemoteReadError> {
    let selectors = query
        .matchers
        .iter()
        .map(|matcher| {
            let op = match matcher.r#type {
                0 => MatchOp::Eq,
                1 => MatchOp::Neq,
                2 => MatchOp::Re,
                3 => MatchOp::Nre,
                other => return Err(RemoteReadError::UnsupportedMatcher(other)),
            };
            Ok(LabelMatcher::new(&matcher.name, op, &matcher.value))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((selectors, query.start_timestamp_ms, query.end_timestamp_ms))
}
