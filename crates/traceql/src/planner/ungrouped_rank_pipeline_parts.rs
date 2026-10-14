use super::{Pipeline, RankFilter, UngroupedRankParts};

pub(crate) fn ungrouped_rank_pipeline_parts(
    pipeline: &[Pipeline],
) -> Option<UngroupedRankParts<'_>> {
    match pipeline {
        [Pipeline::Aggregate(agg), rank_pipeline!(rank)] => Some(UngroupedRankParts {
            aggregate: agg,
            rank,
            filter: None,
        }),
        [
            Pipeline::Aggregate(agg),
            Pipeline::Filter { op, value },
            rank_pipeline!(rank),
        ]
        | [
            Pipeline::Aggregate(agg),
            rank_pipeline!(rank),
            Pipeline::Filter { op, value },
        ] => Some(UngroupedRankParts {
            aggregate: agg,
            rank,
            filter: Some(RankFilter::new(*op, *value)),
        }),
        _ => None,
    }
}
