use krabka_blockstore::{
    MatchOp, QUERY_SHARD_LABEL, QueryShardSelector, SeriesFingerprint, parse_query_shard_selector,
};

use crate::{PromqlError, PromqlLabels as Labels, PromqlMatcher as LabelMatcher, error::Result};

mod all_match;
mod prepare_matchers;
mod prepared_matcher;
mod regex_anchored;
mod row_matches;

pub(crate) use all_match::all_match;
pub(crate) use prepare_matchers::prepare_matchers;
pub(crate) use prepared_matcher::PreparedMatcher;
use regex_anchored::regex_anchored;
pub(crate) use row_matches::row_matches;
