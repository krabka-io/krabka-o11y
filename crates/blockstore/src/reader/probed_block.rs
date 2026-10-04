use super::{Arc, ObjectMeta, ParquetMetaData};

/// One readable block, as a probe found it.
///
/// The probe has to `head` the block and read its footer to know that the
/// block is readable. A scan that keeps both does not have to ask the store
/// again: [`BlockStore`](crate::BlockStore) hands them to `DataFusion`.
#[derive(Clone, Debug)]
pub(crate) struct ProbedBlock {
    /// The object key the index named.
    pub(crate) object_key: String,
    /// The object as the probe's `head` described it.
    pub(crate) meta: ObjectMeta,
    /// The decoded footer.
    pub(crate) metadata: Arc<ParquetMetaData>,
}
