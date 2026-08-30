
/// Cardinality for one label value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LabelValueCardinality {
    pub label_name: String,
    pub label_value: String,
    pub series_count: usize,
}
