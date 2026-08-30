use super::{Pipeline, RankDirection, Result, TraceqlError};

#[derive(Clone, Copy)]
pub(crate) struct RankLimit {
    pub(crate) direction: RankDirection,
    pub(crate) k: usize,
}

pub(crate) fn rank_limit(pipeline: &Pipeline) -> Result<RankLimit> {
    match pipeline {
        Pipeline::TopK(k) => Ok(RankLimit {
            direction: RankDirection::Top,
            k: *k,
        }),
        Pipeline::BottomK(k) => Ok(RankLimit {
            direction: RankDirection::Bottom,
            k: *k,
        }),
        other => Err(TraceqlError::Unsupported(format!(
            "traceql metrics: expected topk/bottomk, got {other:?}"
        ))),
    }
}
