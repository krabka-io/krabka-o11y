/// One flamegraph level.
///
/// The `values` field holds groups of four numbers:
/// `[xOffsetDelta, total, self, nameIndex]`.
#[derive(Clone, Debug, PartialEq)]
pub struct Level {
    pub values: Vec<i64>,
}
