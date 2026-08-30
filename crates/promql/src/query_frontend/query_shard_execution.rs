use super::*;

pub(crate) enum QueryShardExecution {
    Merge(QueryShardReducer),
    Avg {
        sum_query: String,
        count_query: String,
    },
    Moments {
        sum_query: String,
        count_query: String,
        sum_squares_query: String,
        kind: MomentReduction,
    },
    Rank {
        k: usize,
        kind: RankReduction,
        modifier: Option<LabelModifier>,
    },
}
