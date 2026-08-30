use super::Pipeline;

pub(crate) fn is_inert_metric_stage(stage: &Pipeline) -> bool {
    matches!(
        stage,
        Pipeline::Select(_) | Pipeline::Coalesce | Pipeline::With(_)
    )
}
