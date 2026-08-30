use super::{Arc, Mutex, BTreeMap, StoreKey, EdgeCheckpointStore};

#[derive(Clone, Default)]
pub struct InMemoryCheckpointStore {
    pub(crate) inner: Arc<Mutex<BTreeMap<StoreKey, Vec<u8>>>>,
}

impl EdgeCheckpointStore for InMemoryCheckpointStore {
    fn save(&self, tenant: &str, key: &[u8], value: &[u8]) {
        let mut inner = self.inner.lock().expect("checkpoint store mutex poisoned");
        let store_key = (tenant.to_string(), key.to_vec());
        if value.is_empty() {
            inner.remove(&store_key);
        } else {
            inner.insert(store_key, value.to_vec());
        }
    }

    fn load_all(&self, tenant: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
        let inner = self.inner.lock().expect("checkpoint store mutex poisoned");
        inner
            .iter()
            .filter(|((stored_tenant, _), _)| stored_tenant == tenant)
            .map(|((_, key), value)| (key.clone(), value.clone()))
            .collect()
    }

    fn tenants(&self) -> Vec<String> {
        let inner = self.inner.lock().expect("checkpoint store mutex poisoned");
        let mut tenants: Vec<_> = inner
            .keys()
            .map(|(tenant, _)| tenant.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        tenants.sort();
        tenants
    }
}
