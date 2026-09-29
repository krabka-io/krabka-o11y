use super::{BTreeMap, ListedObject, ObjectMeta, ObjectRole, StorageSignal};

/// Every classified object of one listing, by key.
#[derive(Clone, Debug, Default)]
pub struct StorageInventory {
    pub objects: BTreeMap<String, ListedObject>,
}

impl StorageInventory {
    pub fn contains(&self, key: &str) -> bool {
        self.objects.contains_key(key)
    }

    pub fn meta(&self, key: &str) -> Option<&ObjectMeta> {
        self.objects.get(key).map(|listed| &listed.meta)
    }

    /// The objects of `signal`, in key order.
    pub fn of_signal(
        &self,
        signal: StorageSignal,
    ) -> impl Iterator<Item = (&String, &ListedObject)> {
        self.objects
            .iter()
            .filter(move |(_, listed)| listed.object.signal == signal)
    }

    /// The blocks of `signal`, in key order.
    pub fn blocks(&self, signal: StorageSignal) -> impl Iterator<Item = (&String, &ListedObject)> {
        self.of_signal(signal)
            .filter(|(_, listed)| matches!(listed.object.role, ObjectRole::Block(_)))
    }
}
