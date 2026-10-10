/// What [`assign_nested_set`](super::assign_nested_set) does with spans that
/// no root reaches because their parent links form a cycle (for example
/// `A.parent = B`, `B.parent = A`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CycleSpans {
    /// Seed each unreached span as an extra root, so every span gets a valid
    /// `left < right` interval and the `-1` root sentinel parent.
    AssignIntervals,
    /// Leave unreached spans at `{left: 0, right: 0, parent_id: 0}`.
    LeaveUnassigned,
}
