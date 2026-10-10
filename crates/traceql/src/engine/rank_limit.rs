use super::{Pipeline, RankLimit, Result, TraceqlError, rank_stage_limit};

pub(crate) fn rank_limit(pipeline: &Pipeline) -> Result<RankLimit> {
    if matches!(pipeline, Pipeline::TopK(0) | Pipeline::BottomK(0)) {
        return Err(TraceqlError::Plan(
            "metrics rank limit must be positive".into(),
        ));
    }
    rank_stage_limit(pipeline).ok_or_else(|| {
        TraceqlError::Unsupported(format!(
            "traceql metrics: expected topk/bottomk, got {pipeline:?}"
        ))
    })
}
