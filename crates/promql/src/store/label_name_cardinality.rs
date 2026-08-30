
/// Cardinality for one label name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LabelNameCardinality {
    pub name: String,
    pub series_count: usize,
}
