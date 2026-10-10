use super::{Pipeline, RankDirection, Result, TraceqlError};

#[derive(Clone, Copy)]
pub(crate) struct RankLimit {
    pub(crate) direction: RankDirection,
    pub(crate) k: usize,
}

/// The direction and size of a `topk` or `bottomk` stage, or `None` for any
/// other pipeline stage.
pub(crate) fn rank_stage_limit(pipeline: &Pipeline) -> Option<RankLimit> {
    match pipeline {
        Pipeline::TopK(k) => Some(RankLimit {
            direction: RankDirection::Top,
            k: *k,
        }),
        Pipeline::BottomK(k) => Some(RankLimit {
            direction: RankDirection::Bottom,
            k: *k,
        }),
        _ => None,
    }
}

pub(crate) fn rank_limit(pipeline: &Pipeline) -> Result<RankLimit> {
    rank_stage_limit(pipeline).ok_or_else(|| {
        TraceqlError::Unsupported(format!("expected topk/bottomk, got {pipeline:?}"))
    })
}
