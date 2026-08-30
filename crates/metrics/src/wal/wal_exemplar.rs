use super::{Serialize, Deserialize};

/// An exemplar carried alongside a sample.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WalExemplar {
    pub labels: Vec<(String, String)>,
    pub value: f64,
    pub timestamp_ms: i64,
}
