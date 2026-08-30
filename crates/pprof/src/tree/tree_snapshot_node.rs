#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TreeSnapshotNode {
    pub name: String,
    pub total: i64,
    pub self_: i64,
    pub children: Vec<usize>,
}
