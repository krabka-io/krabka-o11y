use promql_parser::parser::MatrixSelector;

/// A matrix selector evaluated at one instant.
#[derive(Clone, Copy)]
pub(crate) struct MatrixSelectorAt<'a> {
    pub(crate) selector: &'a MatrixSelector,
    /// The evaluation instant, in epoch milliseconds.
    pub(crate) time_ms: i64,
}
