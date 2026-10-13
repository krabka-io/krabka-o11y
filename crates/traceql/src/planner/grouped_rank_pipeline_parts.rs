use super::{Aggregate, Field, Pipeline, RankFilter, rank_pipeline};

pub(crate) struct GroupedRankPipelineParts<'a> {
    pub(crate) aggregate: &'a Aggregate,
    pub(crate) by: &'a [Field],
    pub(crate) rank: &'a Pipeline,
    pub(crate) pre_filter: Option<RankFilter>,
    pub(crate) post_filter: Option<RankFilter>,
}

pub(crate) fn grouped_rank_pipeline_parts(
    pipeline: &[Pipeline],
) -> Option<GroupedRankPipelineParts<'_>> {
    match pipeline {
        [
            Pipeline::Aggregate(agg),
            Pipeline::By(by),
            rank_pipeline!(rank),
        ]
        | [
            Pipeline::By(by),
            Pipeline::Aggregate(agg),
            rank_pipeline!(rank),
        ]
        | [
            Pipeline::Aggregate(agg),
            rank_pipeline!(rank),
            Pipeline::By(by),
        ] => Some(GroupedRankPipelineParts {
            aggregate: agg,
            by,
            rank,
            pre_filter: None,
            post_filter: None,
        }),
        [
            Pipeline::Aggregate(agg),
            Pipeline::By(by),
            Pipeline::Filter { op, value },
            rank_pipeline!(rank),
        ]
        | [
            Pipeline::By(by),
            Pipeline::Aggregate(agg),
            Pipeline::Filter { op, value },
            rank_pipeline!(rank),
        ]
        | [
            Pipeline::Aggregate(agg),
            Pipeline::Filter { op, value },
            rank_pipeline!(rank),
            Pipeline::By(by),
        ] => Some(GroupedRankPipelineParts {
            aggregate: agg,
            by,
            rank,
            pre_filter: Some(RankFilter::new(*op, *value)),
            post_filter: None,
        }),
        [
            Pipeline::Aggregate(agg),
            Pipeline::By(by),
            rank_pipeline!(rank),
            Pipeline::Filter { op, value },
        ]
        | [
            Pipeline::By(by),
            Pipeline::Aggregate(agg),
            rank_pipeline!(rank),
            Pipeline::Filter { op, value },
        ]
        | [
            Pipeline::Aggregate(agg),
            rank_pipeline!(rank),
            Pipeline::By(by),
            Pipeline::Filter { op, value },
        ]
        | [
            Pipeline::Aggregate(agg),
            rank_pipeline!(rank),
            Pipeline::Filter { op, value },
            Pipeline::By(by),
        ] => Some(GroupedRankPipelineParts {
            aggregate: agg,
            by,
            rank,
            pre_filter: None,
            post_filter: Some(RankFilter::new(*op, *value)),
        }),
        _ => None,
    }
}
