pub trait EdgeCheckpointStore: Send + Sync {
    fn save(&self, tenant: &str, key: &[u8], value: &[u8]);
    fn load_all(&self, tenant: &str) -> Vec<CheckpointEntry>;
    fn tenants(&self) -> Vec<String>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}
