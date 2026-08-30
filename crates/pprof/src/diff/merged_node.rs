#[derive(Clone, Debug)]
pub(crate) struct MergedNode {
    pub(crate) name: String,
    pub(crate) total_left: i64,
    pub(crate) self_left: i64,
    pub(crate) total_right: i64,
    pub(crate) self_right: i64,
    pub(crate) children: Vec<usize>,
}
