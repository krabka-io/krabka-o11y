use krabka_blockstore::{
    MatchOp, QUERY_SHARD_LABEL, QueryShardSelector, SeriesFingerprint, parse_query_shard_selector,
};

use crate::{PromqlError, PromqlLabels as Labels, PromqlMatcher as LabelMatcher, error::Result};
mod prepared_matcher;
pub(crate) use prepared_matcher::PreparedMatcher;

pub(crate) fn prepare_matchers(matchers: &[LabelMatcher]) -> Result<Vec<PreparedMatcher>> {
    matchers.iter().map(PreparedMatcher::new).collect()
}

pub(crate) fn regex_anchored(pattern: &str) -> Result<regex::Regex> {
    regex::Regex::new(&format!("^(?s:{pattern})$"))
        .map_err(|error| PromqlError::Plan(format!("bad regex `{pattern}`: {error}")))
}

pub(crate) fn all_match(
    fp: SeriesFingerprint,
    labels: &Labels,
    matchers: &[PreparedMatcher],
) -> bool {
    for matcher in matchers {
        if !matcher.matches(fp, labels) {
            return false;
        }
    }
    true
}

pub(crate) fn row_matches(
    fp: SeriesFingerprint,
    labels: &Labels,
    ts_ms: i64,
    matchers: &[PreparedMatcher],
    start_ms: i64,
    end_ms: i64,
) -> bool {
    if ts_ms.cmp(&start_ms).is_lt() {
        return false;
    }
    if ts_ms.cmp(&end_ms).is_gt() {
        return false;
    }
    all_match(fp, labels, matchers)
}
