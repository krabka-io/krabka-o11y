use super::HashMap;

#[derive(Clone, Debug)]
pub(crate) struct Node {
    pub(crate) name: String,
    pub(crate) total: i64,
    pub(crate) self_: i64,
    pub(crate) children: Vec<usize>,
    pub(crate) child_by_name: HashMap<String, usize>,
}
