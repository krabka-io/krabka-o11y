use super::*;

/// `status="ok" | "error"` label for the ingest-request counter family.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct StatusLabel {
    pub status: String,
}
