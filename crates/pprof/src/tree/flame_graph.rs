use super::Level;

/// Pyroscope-compatible single flamegraph projection.
#[derive(Clone, Debug, PartialEq)]
pub struct FlameGraph {
    pub names: Vec<String>,
    pub levels: Vec<Level>,
    pub total: i64,
    pub max_self: i64,
}
