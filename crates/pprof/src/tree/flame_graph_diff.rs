use super::Level;

/// Diff flamegraph type frozen for the next slice.
#[derive(Clone, Debug, PartialEq)]
pub struct FlameGraphDiff {
    pub names: Vec<String>,
    pub levels: Vec<Level>,
    pub left_ticks: i64,
    pub right_ticks: i64,
}
