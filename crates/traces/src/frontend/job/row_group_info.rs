use super::ByteSize;

/// One candidate row-group of a backend block.
#[derive(Clone, Debug, PartialEq)]
pub struct RowGroupInfo {
    pub index: u32,
    /// This row-group's compressed size on the object store.
    pub compressed: ByteSize,
}
