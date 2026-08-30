use super::*;

pub(crate) fn grouped_rank_pipeline_parts(
    pipeline: &[Pipeline],
) -> Option<(&Aggregate, &[Field], &Pipeline, RankFilter, RankFilter)> {
    match pipeline {
        [
            Pipeline::Aggregate(agg),
            Pipeline::By(by),
            rank @ (Pipeline::TopK(_) | Pipeline::BottomK(_)),
        ]
        | [
            Pipeline::By(by),
            Pipeline::Aggregate(agg),
            rank @ (Pipeline::TopK(_) | Pipeline::BottomK(_)),
        ]
        | [
            Pipeline::Aggregate(agg),
            rank @ (Pipeline::TopK(_) | Pipeline::BottomK(_)),
            Pipeline::By(by),
        ] => Some((agg, by, rank, None, None)),
        [
            Pipeline::Aggregate(agg),
            Pipeline::By(by),
            Pipeline::Filter { op, value },
            rank @ (Pipeline::TopK(_) | Pipeline::BottomK(_)),
        ]
        | [
            Pipeline::By(by),
            Pipeline::Aggregate(agg),
            Pipeline::Filter { op, value },
            rank @ (Pipeline::TopK(_) | Pipeline::BottomK(_)),
        ]
        | [
            Pipeline::Aggregate(agg),
            Pipeline::Filter { op, value },
            rank @ (Pipeline::TopK(_) | Pipeline::BottomK(_)),
            Pipeline::By(by),
        ] => Some((agg, by, rank, Some((*op, *value)), None)),
        [
            Pipeline::Aggregate(agg),
            Pipeline::By(by),
            rank @ (Pipeline::TopK(_) | Pipeline::BottomK(_)),
            Pipeline::Filter { op, value },
        ]
        | [
            Pipeline::By(by),
            Pipeline::Aggregate(agg),
            rank @ (Pipeline::TopK(_) | Pipeline::BottomK(_)),
            Pipeline::Filter { op, value },
        ]
        | [
            Pipeline::Aggregate(agg),
            rank @ (Pipeline::TopK(_) | Pipeline::BottomK(_)),
            Pipeline::By(by),
            Pipeline::Filter { op, value },
        ]
        | [
            Pipeline::Aggregate(agg),
            rank @ (Pipeline::TopK(_) | Pipeline::BottomK(_)),
            Pipeline::Filter { op, value },
            Pipeline::By(by),
        ] => Some((agg, by, rank, None, Some((*op, *value)))),
        _ => None,
    }
}
