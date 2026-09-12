use super::Error;

#[derive(Debug, Error)]
pub enum RemoteReadError {
    #[error("snappy decode failed: {0}")]
    SnappyDecode(String),
    #[error("snappy decoded body exceeds max_output={0}")]
    SnappyOutputTooLarge(usize),
    #[error("snappy encode failed: {0}")]
    SnappyEncode(String),
    #[error("protobuf decode failed: {0}")]
    Decode(String),
    #[error("protobuf encode failed: {0}")]
    Encode(String),
    #[error("unsupported remote_read matcher type {0}")]
    UnsupportedMatcher(i32),
    #[error("server does not support any requested remote_read response type: {0:?}")]
    UnsupportedResponseTypes(Vec<i32>),
    #[error("remote_read samples are out of timestamp order: {previous} then {current}")]
    UnorderedSamples { previous: i64, current: i64 },
    #[error("remote_read query count exceeds i64 at index {0}")]
    TooManyQueries(usize),
}
