
pub trait EdgeCheckpointStore: Send + Sync {
    fn save(&self, tenant: &str, key: &[u8], value: &[u8]);
    fn load_all(&self, tenant: &str) -> Vec<(Vec<u8>, Vec<u8>)>;
    fn tenants(&self) -> Vec<String>;
}
