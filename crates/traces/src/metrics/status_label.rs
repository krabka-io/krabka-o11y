use super::EncodeLabelSet;

/// Ingest request outcome label, `status="ok"` or `status="error"`.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct StatusLabel {
    pub status: String,
}
