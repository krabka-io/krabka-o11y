use super::*;

/// Parsed Prometheus `.test` file.
#[derive(Clone, Debug, PartialEq)]
pub struct TestFile {
    /// Top-level statements in file order.
    pub statements: Vec<Statement>,
}
