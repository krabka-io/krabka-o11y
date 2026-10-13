#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct StoreKey {
    pub(crate) tenant: String,
    pub(crate) key: Vec<u8>,
}
