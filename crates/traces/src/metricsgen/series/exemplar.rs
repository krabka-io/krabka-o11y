
/// Prometheus exemplar attached to a sample.
#[derive(Clone, Debug, PartialEq)]
pub struct Exemplar {
    pub value: f64,
    pub labels: Vec<(String, String)>,
    pub timestamp_ms: i64,
}
