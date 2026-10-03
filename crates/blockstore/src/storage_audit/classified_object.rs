use super::{ObjectRole, StorageSignal};

/// An object key that matched one signal's key grammar.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClassifiedObject {
    pub signal: StorageSignal,
    /// The unescaped tenant, or `None` for shared state.
    pub tenant: Option<String>,
    pub role: ObjectRole,
}

impl ClassifiedObject {
    pub fn new(signal: StorageSignal, tenant: Option<String>, role: ObjectRole) -> Self {
        Self {
            signal,
            tenant,
            role,
        }
    }

    /// The block shape, when the object is a block.
    pub const fn block(&self) -> Option<&super::BlockShape> {
        match &self.role {
            ObjectRole::Block(shape) => Some(shape),
            _ => None,
        }
    }
}
