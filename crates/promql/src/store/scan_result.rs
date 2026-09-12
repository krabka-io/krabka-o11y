use super::SessionContext;

/// A leaf scan result with up to two `DataFusion` tables registered.
pub struct ScanResult {
    pub ctx: SessionContext,
    pub float_table: Option<String>,
    pub histogram_table: Option<String>,

    /// One line for each block the scan answered without.
    ///
    /// A store that leaves a block out and says nothing returns a short answer
    /// that looks whole. The engine raises each line as a `PromQL` warning
    /// annotation, which is the channel Prometheus reports a partial result
    /// through. An empty vector means the scan read every block the index
    /// named.
    pub warnings: Vec<String>,
}
