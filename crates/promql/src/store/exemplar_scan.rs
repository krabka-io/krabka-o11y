use super::ExemplarRecord;

/// The exemplars a store returned, and the blocks it answered without.
///
/// [`Self::warnings`] carries one line for each block the scan left out. See
/// [`ScanResult::warnings`](super::ScanResult::warnings) for why a store
/// reports a short answer instead of hiding it.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ExemplarScan {
    /// The exemplars of the matched series, in `(series, timestamp)` order.
    pub exemplars: Vec<ExemplarRecord>,

    /// One line for each block the scan answered without.
    pub warnings: Vec<String>,
}
