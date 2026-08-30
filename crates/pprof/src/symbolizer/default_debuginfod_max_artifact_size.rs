use super::{ByteSize, mebibytes};

/// Default maximum size of a debuginfo artifact downloaded from a debuginfod
/// server.
pub const DEFAULT_DEBUGINFOD_MAX_ARTIFACT_SIZE: ByteSize = mebibytes(512);
