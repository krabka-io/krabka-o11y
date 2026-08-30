use krabka_blockstore::{
    LabelMatcher, Labels, MatchOp, QUERY_SHARD_LABEL, QueryShardSelector, SeriesFingerprint,
    parse_query_shard_selector,
};

use crate::{PromqlError, error::Result};

// === split-modules: generated submodules ===
mod all_match;
mod prepare_matchers;
mod prepared_matcher;
mod regex_anchored;
mod row_matches;

pub (super) use all_match::all_match;
pub (super) use prepare_matchers::prepare_matchers;
pub (super) use prepared_matcher::PreparedMatcher;
use regex_anchored::regex_anchored;
pub (super) use row_matches::row_matches;
