//! Label matchers.

use serde::{Deserialize, Serialize};

use crate::labels::SeriesFingerprint;

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn parse_query_shard_selector_accepts_inclusive_upper_bound() {
        let selector = parse_query_shard_selector("1_of_1").unwrap();

        assert2::assert!(selector == QueryShardSelector { index: 1, total: 1 });
        assert2::assert!(selector.matches(42));
    }

    #[test]
    fn parse_query_shard_selector_rejects_zero_and_out_of_range_bounds() {
        for (_name, value) in [
            ("zero index", "0_of_1"),
            ("zero total", "1_of_0"),
            ("index exceeds total", "2_of_1"),
            ("larger index exceeds total", "3_of_2"),
            ("malformed selector", "not-a-shard"),
        ] {
            assert2::assert!(parse_query_shard_selector(value).is_err());
        }
    }
}

// === split-modules: generated submodules ===
mod label_matcher;
mod match_op;
mod parse_query_shard_selector;
mod query_shard_label;
mod query_shard_selector;

pub use label_matcher::LabelMatcher;
pub use match_op::MatchOp;
pub use parse_query_shard_selector::parse_query_shard_selector;
pub use query_shard_label::QUERY_SHARD_LABEL;
pub use query_shard_selector::QueryShardSelector;
