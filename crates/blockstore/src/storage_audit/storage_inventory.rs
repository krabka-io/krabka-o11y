use super::{BTreeMap, ListedObject, ObjectMeta, ObjectRole, StorageSignal};

#[derive(Clone, Copy)]
pub struct InventoryObjectRef<'a> {
    pub key: &'a String,
    pub listed: &'a ListedObject,
}

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
    pub fn of_signal(&self, signal: StorageSignal) -> impl Iterator<Item = InventoryObjectRef<'_>> {
        self.objects.keys().filter_map(move |key| {
            let listed = &self.objects[key];
            (listed.object.signal == signal).then_some(InventoryObjectRef { key, listed })
        })
    }

    /// The blocks of `signal`, in key order.
    pub fn blocks(&self, signal: StorageSignal) -> impl Iterator<Item = InventoryObjectRef<'_>> {
        self.of_signal(signal)
            .filter(|entry| matches!(entry.listed.object.role, ObjectRole::Block(_)))
    }
}
