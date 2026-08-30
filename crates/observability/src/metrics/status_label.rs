use super::*;

/// Ingest request outcome label (`status="ok"|"error"`).
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct StatusLabel {
    pub status: String,
}
