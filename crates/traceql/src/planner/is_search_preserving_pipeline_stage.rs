use super::{Aggregate, Pipeline};

pub(crate) fn is_search_preserving_pipeline_stage(stage: &Pipeline) -> bool {
    matches!(
        stage,
        Pipeline::By(_)
            | Pipeline::Select(_)
            | Pipeline::Coalesce
            | Pipeline::With(_)
            | Pipeline::Aggregate(
                Aggregate::Count
                    | Aggregate::Sum(_)
                    | Aggregate::Avg(_)
                    | Aggregate::Min(_)
                    | Aggregate::Max(_)
            )
    )
}
