#[derive(Clone, Debug)]
pub(crate) struct Bar {
    pub(crate) node: Option<usize>,
    pub(crate) name: String,
    pub(crate) total_left: i64,
    pub(crate) self_left: i64,
    pub(crate) total_right: i64,
    pub(crate) self_right: i64,
    pub(crate) x_left: i64,
    pub(crate) x_right: i64,
}
