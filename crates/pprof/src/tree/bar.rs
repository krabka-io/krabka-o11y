#[derive(Clone, Debug)]
pub(crate) struct Bar {
    pub(crate) node: Option<usize>,
    pub(crate) name: String,
    pub(crate) total: i64,
    pub(crate) self_: i64,
    pub(crate) x_start: i64,
    pub(crate) level: usize,
}
