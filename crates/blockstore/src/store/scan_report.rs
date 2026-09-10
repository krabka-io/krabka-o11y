use super::SkippedBlock;

/// What a scan registered, and what it could not read.
///
/// A scan that skips a block and says nothing has delivered a wrong answer
/// with a straight face: the caller cannot tell an empty window from an
/// unreadable one. Every skip is listed here so the HTTP layer can put it in
/// the `warnings` of the response body, the way Loki and Tempo report a
/// partial result.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ScanReport {
    /// Whether any block was registered. `false` means the table exists but is
    /// empty — no candidate blocks, or none of them readable.
    pub registered: bool,

    /// The blocks left out, in the order the index named them.
    pub skipped: Vec<SkippedBlock>,
}

impl ScanReport {
    /// Whether the scan answered from fewer blocks than the index named, and
    /// so the result the caller is about to return is incomplete.
    #[must_use]
    pub fn is_partial(&self) -> bool {
        !self.skipped.is_empty()
    }

    /// One line per skipped block, ready to be returned as response warnings.
    #[must_use]
    pub fn warnings(&self) -> Vec<String> {
        self.skipped
            .iter()
            .map(std::string::ToString::to_string)
            .collect()
    }
}
