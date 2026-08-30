use super::{Aggregate, Pipeline, RankFilter};

pub(crate) type UngroupedRankParts<'a> = (&'a Aggregate, &'a Pipeline, RankFilter);
