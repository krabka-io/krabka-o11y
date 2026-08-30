use super::{Field, Pipeline, is_search_preserving_aggregate};

pub(crate) fn grouped_no_filter_by(pipeline: &[Pipeline]) -> Option<&[Field]> {
    match pipeline {
        [Pipeline::Aggregate(agg), Pipeline::By(by)]
        | [Pipeline::By(by), Pipeline::Aggregate(agg)]
        | [
            Pipeline::Aggregate(agg),
            Pipeline::By(by),
            Pipeline::Coalesce,
        ]
        | [
            Pipeline::By(by),
            Pipeline::Aggregate(agg),
            Pipeline::Coalesce,
        ] if is_search_preserving_aggregate(agg) => Some(by),
        _ => None,
    }
}
