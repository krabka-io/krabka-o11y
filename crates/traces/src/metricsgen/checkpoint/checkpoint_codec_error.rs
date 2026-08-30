use super::*;

#[derive(Debug, thiserror::Error)]
pub enum CheckpointCodecError {
    #[error("truncated checkpoint key")]
    Truncated,
    #[error("invalid utf8 in checkpoint tenant")]
    Utf8,
    #[error("bad trace id length in checkpoint key")]
    BadTraceId,
    #[error("bad edge id length in checkpoint key")]
    BadEdgeId,
    #[error("bad connection type in checkpoint value")]
    BadConnectionType,
}
