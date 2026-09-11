use super::{EncodeLabelSet, ObjectStoreOperation};

/// The `operation` label the object-store families carry.
///
/// The value is a `&'static str` taken from [`ObjectStoreOperation::as_str`],
/// so building a label set to reach a series allocates nothing.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ObjectStoreOperationLabel {
    pub operation: &'static str,
}

impl From<ObjectStoreOperation> for ObjectStoreOperationLabel {
    fn from(operation: ObjectStoreOperation) -> Self {
        Self {
            operation: operation.as_str(),
        }
    }
}
