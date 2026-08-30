
#[derive(Debug, thiserror::Error)]
pub enum HaElectionRecordError {
    #[error("HA election record encode failed: {0}")]
    Encode(String),

    #[error("HA election record decode failed: {0}")]
    Decode(String),
}
