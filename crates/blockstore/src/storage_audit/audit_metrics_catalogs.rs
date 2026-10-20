use serde::Deserialize;

use super::{
    Arc, BTreeMap, MetricsBlockKind, MetricsIndexManifest, ObjectRole, ObjectStore,
    StorageAuditError, StorageFinding, StorageFindingKind, StorageInventory, StorageSignal,
    read_small_object,
};

#[derive(Deserialize)]
struct Catalog {
    format_version: u32,
    manifests: Vec<Reference>,
}

#[derive(Deserialize)]
struct Reference {
    index_key: String,
    min_ts: i64,
    max_ts: i64,
    kind: MetricsBlockKind,
}

fn catalog_key(tenant: &str, reference: &Reference) -> String {
    crate::metrics_manifest_catalog_key(
        tenant,
        &reference.index_key,
        reference.min_ts,
        reference.max_ts,
        reference.kind == MetricsBlockKind::Metadata,
    )
}

pub(super) async fn audit_metrics_catalogs(
    store: &Arc<dyn ObjectStore>,
    inventory: &StorageInventory,
    manifests: &BTreeMap<String, MetricsIndexManifest>,
) -> Result<Vec<StorageFinding>, StorageAuditError> {
    let mut findings = Vec::new();
    let mut present = std::collections::BTreeSet::new();
    for entry in inventory.of_signal(StorageSignal::Metrics) {
        if entry.listed.object.role != ObjectRole::MetricsCatalog {
            continue;
        }
        let tenant = entry.listed.object.tenant.as_deref().unwrap_or_default();
        let finding = |kind, detail| {
            StorageFinding::new(
                kind,
                StorageSignal::Metrics,
                Some(tenant.to_owned()),
                entry.key,
                detail,
            )
        };
        let Some(bytes) = read_small_object(store, entry.key).await? else {
            continue;
        };
        present.insert(entry.key.clone());
        let catalog = match serde_json::from_slice::<Catalog>(&bytes) {
            Ok(catalog) => catalog,
            Err(error) => {
                findings.push(finding(
                    StorageFindingKind::UnreadableManifest,
                    error.to_string(),
                ));
                continue;
            }
        };
        if catalog.format_version != 1 {
            findings.push(finding(
                StorageFindingKind::UnsupportedFormat,
                format!("catalog version {}, expected 1", catalog.format_version),
            ));
            continue;
        }
        let [reference] = catalog.manifests.as_slice() else {
            findings.push(finding(
                StorageFindingKind::IndexMismatch,
                "a summary must contain exactly one manifest reference".to_owned(),
            ));
            continue;
        };
        if catalog_key(tenant, reference) != *entry.key {
            findings.push(finding(
                StorageFindingKind::IndexMismatch,
                "catalog key disagrees with its manifest reference".to_owned(),
            ));
            continue;
        }
        // Retired manifests leave summaries until the orphan sweep. These
        // summaries do not make a block live and need no dangling finding.
        if let Some(manifest) = manifests.get(&reference.index_key)
            && (manifest.tenant != tenant
                || manifest.min_ts != reference.min_ts
                || manifest.max_ts != reference.max_ts
                || manifest.kind != reference.kind)
        {
            findings.push(finding(
                StorageFindingKind::IndexMismatch,
                format!(
                    "catalog disagrees with live manifest `{}`",
                    reference.index_key
                ),
            ));
        }
    }
    for manifest in manifests.values() {
        // An import is not queryable before its publication marker exists.
        if let Some((directory, _)) = manifest.index_key.rsplit_once('/')
            && crate::is_metrics_import_directory(directory)
            && !inventory.contains(&format!("{directory}/_published"))
        {
            continue;
        }
        let key = crate::metrics_manifest_catalog_key(
            &manifest.tenant,
            &manifest.index_key,
            manifest.min_ts,
            manifest.max_ts,
            manifest.kind == MetricsBlockKind::Metadata,
        );
        if !present.contains(&key) {
            findings.push(StorageFinding::new(
                StorageFindingKind::UnreadableManifest,
                StorageSignal::Metrics,
                Some(manifest.tenant.clone()),
                &key,
                format!(
                    "live manifest `{}` has no catalog summary",
                    manifest.index_key
                ),
            ));
        }
    }
    Ok(findings)
}
