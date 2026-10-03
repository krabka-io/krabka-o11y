use super::{
    Arc, ErasureRequest, ObjectRole, ObjectStore, StorageAuditError, StorageFinding,
    StorageFindingKind, StorageInventory, StorageSignal, TenantDeletionMarker,
    escape_object_path_segment, read_small_object,
};

/// Decodes every metrics delete marker and erasure request.
///
/// A record that does not decode, or that names another tenant than its key
/// does, is `unreadable_delete_state`. The compactor would skip or refuse
/// it, and the delete it stands for would never happen. A tenant deletion
/// marker that lists an object outside the metrics block and upload prefixes
/// of its tenant is also `unreadable_delete_state`, because the delete
/// worker would remove data of another tenant.
///
/// Logs delete requests live on the local disk of the logs service, not in
/// the object store, so this audit does not reach them.
///
/// # Errors
/// Returns [`StorageAuditError::ObjectStore`] when a read fails for a reason
/// other than absence.
pub async fn audit_delete_state(
    store: &Arc<dyn ObjectStore>,
    inventory: &StorageInventory,
) -> Result<Vec<StorageFinding>, StorageAuditError> {
    let mut findings = Vec::new();
    for (key, listed) in inventory.of_signal(StorageSignal::Metrics) {
        let tenant = listed.object.tenant.clone().unwrap_or_default();
        let problem = match &listed.object.role {
            ObjectRole::TenantDeletion => {
                let Some(bytes) = read_small_object(store, key).await? else {
                    continue;
                };
                match serde_json::from_slice::<TenantDeletionMarker>(&bytes) {
                    Ok(marker) if marker.tenant_id != tenant => {
                        Some(format!("names tenant `{}`", marker.tenant_id))
                    }
                    Ok(marker) => foreign_marker_object(&marker)
                        .map(|object| format!("lists object `{object}` of another tenant")),
                    Err(error) => Some(error.to_string()),
                }
            }
            ObjectRole::ErasureRequest { id } => {
                let Some(bytes) = read_small_object(store, key).await? else {
                    continue;
                };
                match serde_json::from_slice::<ErasureRequest>(&bytes) {
                    Ok(request) if request.tenant == tenant && &request.id == id => None,
                    Ok(request) => Some(format!(
                        "names tenant `{}` and id `{}`",
                        request.tenant, request.id
                    )),
                    Err(error) => Some(error.to_string()),
                }
            }
            _ => continue,
        };
        if let Some(detail) = problem {
            findings.push(StorageFinding::new(
                StorageFindingKind::UnreadableDeleteState,
                StorageSignal::Metrics,
                listed.object.tenant.clone(),
                key,
                detail,
            ));
        }
    }
    Ok(findings)
}

fn foreign_marker_object(marker: &TenantDeletionMarker) -> Option<&str> {
    let escaped = escape_object_path_segment(&marker.tenant_id);
    let blocks = format!("metrics/{escaped}/");
    let uploads = format!("mimir-block-uploads/{escaped}/");
    marker
        .objects
        .iter()
        .map(String::as_str)
        .find(|object| !object.starts_with(&blocks) && !object.starts_with(&uploads))
}
