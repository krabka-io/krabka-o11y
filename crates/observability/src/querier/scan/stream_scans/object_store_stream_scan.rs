use super::{BlockDescriptor, Value};

pub(crate) struct ObjectStoreStreamScan {
    pub(crate) value: Value,
    pub(crate) scanned_blocks: Vec<BlockDescriptor>,
}
