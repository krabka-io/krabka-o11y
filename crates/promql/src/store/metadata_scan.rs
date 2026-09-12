use super::MetadataRecord;

/// The metric metadata a store returned, and the blocks it answered without.
///
/// [`Self::warnings`] carries one line for each block the scan left out. See
/// [`ScanResult::warnings`](super::ScanResult::warnings) for why a store
/// reports a short answer instead of hiding it.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataScan {
    /// The metadata records of the matched metric families.
    pub metadata: Vec<MetadataRecord>,

    /// One line for each block the scan answered without.
    pub warnings: Vec<String>,
}
