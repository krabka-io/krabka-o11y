use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum QuerierIndexSource {
    LocalManifest,
    TenantObjectStoreManifest,
    TenantObjectStoreShards,
}
