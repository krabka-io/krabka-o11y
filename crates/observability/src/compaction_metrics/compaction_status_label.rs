use super::EncodeLabelSet;

/// The `status` label the compaction-run counter carries.
///
/// It takes exactly the two values `ok` and `error`, so the family holds two
/// series.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct CompactionStatusLabel {
    pub status: &'static str,
}

impl CompactionStatusLabel {
    /// The label for a pass that finished.
    pub const OK: Self = Self { status: "ok" };
    /// The label for a pass that failed.
    pub const ERROR: Self = Self { status: "error" };

    /// The label for an outcome.
    #[must_use]
    pub const fn for_outcome(ok: bool) -> Self {
        if ok { Self::OK } else { Self::ERROR }
    }
}
