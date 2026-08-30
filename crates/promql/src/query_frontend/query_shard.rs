use super::*;

/// One Mimir-compatible query shard.
///
/// Shards are one-based on the wire: `1_of_3`, `2_of_3`, ...
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct QueryShard {
    pub index: usize,
    pub total: usize,
}

impl QueryShard {
    #[must_use]
    pub fn selector_value(self) -> String {
        format!("{}_of_{}", self.index, self.total)
    }

    #[must_use]
    pub fn matcher(self) -> LabelMatcher {
        LabelMatcher {
            name: QUERY_SHARD_LABEL.to_string(),
            op: MatchOp::Eq,
            value: self.selector_value(),
        }
    }
}
