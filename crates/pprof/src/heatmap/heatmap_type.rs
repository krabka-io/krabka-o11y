#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Heatmap {
    pub start_ms: i64,
    pub end_ms: i64,
    pub time_buckets: usize,
    pub value_buckets: usize,
    pub min_value: i64,
    pub max_value: i64,
    pub counts: Vec<Vec<u64>>,
}
