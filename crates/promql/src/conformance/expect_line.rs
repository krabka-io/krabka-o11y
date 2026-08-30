use super::*;

/// One expected output line.
#[derive(Clone, Debug, PartialEq)]
pub struct ExpectLine {
    /// Metric selector text, which includes the labels when they are present.
    pub metric: String,
    /// Expected sample slots.
    ///
    /// An instant evaluation must contain one float value. A range evaluation
    /// uses one slot per step.
    pub values: Vec<SampleSpec>,
}
