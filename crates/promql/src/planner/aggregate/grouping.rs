use super::*;

/// How a `PromQL` aggregation selects its grouping labels.
#[derive(Clone, Debug)]
pub enum Grouping {
    /// `by (labels...)`: group by exactly these label columns.
    By(Vec<String>),
    /// `without (labels...)`: group by all input label columns except these and
    /// except `__name__`, which `without` always drops.
    Without(Vec<String>),
}
