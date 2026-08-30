use super::*;

/// Minimal row-group metadata used by query frontends to shard block scans.
///
/// There is no `Eq`. [`ByteSize`] stores `f64`, so it is only `PartialEq`.
/// Nothing keys a map or a set on row-group metadata, so the derive is
/// unused.
#[derive(Clone, Debug, PartialEq)]
pub struct RowGroupMeta {
    pub index: usize,
    pub compressed: ByteSize,
}
