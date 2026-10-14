use super::{LabelMatcher, MatchOp, RemoteReadError, v1};

/// The matcher set and inclusive time range of one remote read query.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteReadSelectors {
    pub selectors: Vec<LabelMatcher>,
    pub start_timestamp_ms: i64,
    pub end_timestamp_ms: i64,
}

/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn matchers_to_selectors(query: &v1::Query) -> Result<RemoteReadSelectors, RemoteReadError> {
    let selectors = query
        .matchers
        .iter()
        .map(|matcher| {
            let op =
                MatchOp::try_from(matcher.r#type).map_err(RemoteReadError::UnsupportedMatcher)?;
            Ok(LabelMatcher::new(&matcher.name, op, &matcher.value))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RemoteReadSelectors {
        selectors,
        start_timestamp_ms: query.start_timestamp_ms,
        end_timestamp_ms: query.end_timestamp_ms,
    })
}
