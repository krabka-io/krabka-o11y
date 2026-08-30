use super::*;

pub(crate) fn ungrouped_rank_pipeline_parts(pipeline: &[Pipeline]) -> Option<UngroupedRankParts<'_>> {
    match pipeline {
        [
            Pipeline::Aggregate(agg),
            rank @ (Pipeline::TopK(_) | Pipeline::BottomK(_)),
        ] => Some((agg, rank, None)),
        [
            Pipeline::Aggregate(agg),
            Pipeline::Filter { op, value },
            rank @ (Pipeline::TopK(_) | Pipeline::BottomK(_)),
        ]
        | [
            Pipeline::Aggregate(agg),
            rank @ (Pipeline::TopK(_) | Pipeline::BottomK(_)),
            Pipeline::Filter { op, value },
        ] => Some((agg, rank, Some((*op, *value)))),
        _ => None,
    }
}
