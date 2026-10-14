use super::{Aggregate, Pipeline, RankFilter};

pub(crate) struct UngroupedRankParts<'a> {
    pub(crate) aggregate: &'a Aggregate,
    pub(crate) rank: &'a Pipeline,
    pub(crate) filter: Option<RankFilter>,
}
