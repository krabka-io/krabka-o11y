use super::{Arc, BTreeMap, EdgeCheckpointStore, Mutex, StoreKey};

#[derive(Clone, Default)]
pub struct InMemoryCheckpointStore {
    pub(crate) inner: Arc<Mutex<BTreeMap<StoreKey, Vec<u8>>>>,
}

impl EdgeCheckpointStore for InMemoryCheckpointStore {
    fn save(&self, tenant: &str, key: &[u8], value: &[u8]) {
        let mut inner = self.inner.lock().expect("checkpoint store mutex poisoned");
        let store_key = StoreKey {
            tenant: tenant.to_string(),
            key: key.to_vec(),
        };
        if value.is_empty() {
            inner.remove(&store_key);
        } else {
            inner.insert(store_key, value.to_vec());
        }
    }

    fn load_all(&self, tenant: &str) -> Vec<super::CheckpointEntry> {
        let inner = self.inner.lock().expect("checkpoint store mutex poisoned");
        inner
            .iter()
            .filter(|entry| entry.0.tenant == tenant)
            .map(|entry| super::CheckpointEntry {
                key: entry.0.key.clone(),
                value: entry.1.clone(),
            })
            .collect()
    }

    fn tenants(&self) -> Vec<String> {
        let inner = self.inner.lock().expect("checkpoint store mutex poisoned");
        let mut tenants: Vec<_> = inner
            .keys()
            .map(|key| key.tenant.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        tenants.sort();
        tenants
    }
}
