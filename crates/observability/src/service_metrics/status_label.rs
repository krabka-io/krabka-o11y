use super::EncodeLabelSet;

/// Request-outcome label, such as `status="ok"` or `status="error"`.
///
/// The profiles symbolizer also uses it for cache lookups
/// (`status="hit"|"miss"`).
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct StatusLabel {
    pub status: String,
}
