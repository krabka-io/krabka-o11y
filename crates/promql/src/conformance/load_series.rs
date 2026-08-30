use super::*;

/// One series in a `load` statement.
#[derive(Clone, Debug, PartialEq)]
pub struct LoadSeries {
    /// Metric selector text, which includes the labels when they are present.
    pub metric: String,
    /// Expanded sample values.
    pub values: Vec<SampleSpec>,
}
