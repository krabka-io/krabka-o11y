use super::Deserialize;

/// A Mimir tenant deletion marker, as `krabka-metrics-service` persists it
/// at `mimir-tenant-deletions/{tenant}.json`.
///
/// The blockstore does not depend on the metrics service, so the shape is
/// restated here.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantDeletionMarker {
    pub tenant_id: String,
    pub objects: Vec<String>,
}
