//! Tenant/time catalog refreshes. Catalogs are hints; `.index` existence decides
//! liveness, and an import marker supplies the whole atomic publication.

use std::{collections::BTreeMap, sync::Arc};

use futures::{StreamExt, TryStreamExt};
use krabka_metrics::{
    CompactionIndexManifest,
    manifest_catalog::{
        MetricManifestCatalog, MetricManifestReference, metric_manifest_catalog_key_overlaps,
        metric_manifest_catalog_prefixes,
    },
};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};

use crate::MetricsServiceError;

type ManifestCache = tokio::sync::RwLock<BTreeMap<String, Arc<CompactionIndexManifest>>>;

#[tracing::instrument(
    level = "debug", name = "metrics.manifests.load_tenant", skip_all,
    fields(tenant = %tenant, start_ms, end_ms, manifests = tracing::field::Empty), err
)]
pub(crate) async fn load_tenant_manifests(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    tenant: &str,
    start_ms: i64,
    end_ms: i64,
    cache: &ManifestCache,
) -> Result<Vec<Arc<CompactionIndexManifest>>, MetricsServiceError> {
    // Retry once after a retirement to include its replacement, as the global
    // loader does. Missing authoritative keys never resurrect cached blocks.
    for attempt in 0..2 {
        let (references, mut missing) =
            list_references(store, prefix, tenant, start_ms, end_ms).await?;
        let mut reads = futures::stream::iter(references.into_values())
            .map(|reference| async move {
                let key = Path::from(reference.index_key.as_str());
                let cached = cache.read().await.get(&reference.index_key).cloned();
                let manifest = if let Some(cached) = cached {
                    match store.head(&key).await {
                        Ok(_) => Some(cached),
                        Err(object_store::Error::NotFound { .. }) => None,
                        Err(error) => return Err(error.into()),
                    }
                } else {
                    match store.get(&key).await {
                        Ok(object) => {
                            let manifest = CompactionIndexManifest::decode(&object.bytes().await?)?;
                            Some(Arc::new(manifest))
                        }
                        Err(error @ object_store::Error::NotFound { .. }) => {
                            match store.head(&key).await {
                                Ok(_) => return Err(error.into()),
                                Err(object_store::Error::NotFound { .. }) => None,
                                Err(error) => return Err(error.into()),
                            }
                        }
                        Err(error) => return Err(error.into()),
                    }
                };
                if let Some(manifest) = &manifest
                    && (manifest.tenant != tenant
                        || manifest.index_key != reference.index_key
                        || manifest.min_ts != reference.min_ts
                        || manifest.max_ts != reference.max_ts
                        || manifest.kind != reference.kind)
                {
                    return Err(MetricsServiceError::Manifest(format!(
                        "catalog does not match manifest {}",
                        reference.index_key
                    )));
                }
                Ok::<_, MetricsServiceError>((reference.index_key, manifest))
            })
            .buffered(4);
        let mut manifests = Vec::new();
        let mut fetched = BTreeMap::new();
        while let Some(result) = reads.next().await {
            let (key, manifest) = result?;
            if let Some(manifest) = manifest {
                fetched.insert(key, Arc::clone(&manifest));
                manifests.push(manifest);
            } else {
                missing = true;
            }
        }
        if missing && attempt == 0 {
            continue;
        }
        *cache.write().await = fetched;
        tracing::Span::current().record("manifests", manifests.len());
        return Ok(manifests);
    }
    unreachable!("the second refresh returns its live manifests")
}

async fn list_references(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    tenant: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<(BTreeMap<String, MetricManifestReference>, bool), MetricsServiceError> {
    let prefixes = metric_manifest_catalog_prefixes(prefix, tenant, start_ms, end_ms);
    let listings = futures::stream::iter(prefixes)
        .map(|prefix| async move {
            store
                .list(Some(&prefix))
                .try_filter(|object| {
                    futures::future::ready(
                        object.location.as_ref().ends_with(".catalog")
                            && metric_manifest_catalog_key_overlaps(
                                object.location.as_ref(),
                                start_ms,
                                end_ms,
                            ),
                    )
                })
                .try_collect::<Vec<_>>()
                .await
        })
        .buffer_unordered(8)
        .try_collect::<Vec<_>>()
        .await?;
    let mut summaries = futures::stream::iter(listings.into_iter().flatten())
        .map(|object| async move {
            match store.get(&object.location).await {
                Ok(object) => Ok(Some(MetricManifestCatalog::decode(&object.bytes().await?)?)),
                // Cleanup can retire a listed hint after its replacement is
                // published. Relist rather than omit that replacement.
                Err(error @ object_store::Error::NotFound { .. }) => {
                    match store.head(&object.location).await {
                        Ok(_) => Err(MetricsServiceError::from(error)),
                        Err(object_store::Error::NotFound { .. }) => Ok(None),
                        Err(error) => Err(MetricsServiceError::from(error)),
                    }
                }
                Err(error) => Err(MetricsServiceError::from(error)),
            }
        })
        .buffered(4);
    let mut references = BTreeMap::new();
    let mut missing_summaries = false;
    let mut imports = std::collections::BTreeSet::new();
    while let Some(catalog) = summaries.next().await {
        let Some(catalog) = catalog? else {
            missing_summaries = true;
            continue;
        };
        for reference in catalog.manifests {
            if let Some(marker) = reference.publication_marker() {
                imports.insert(marker);
            } else if reference.overlaps(start_ms, end_ms) {
                references.insert(reference.index_key.clone(), reference);
            }
        }
    }
    // Each marker body contains every reference, even if catalog publication
    // raced the listing. Never expose a partial set from an imported block.
    let mut publications = futures::stream::iter(imports)
        .map(|marker| async move {
            match store.get(&Path::from(marker)).await {
                Ok(object) => Ok(Some(MetricManifestCatalog::decode(&object.bytes().await?)?)),
                Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(error) => Err(MetricsServiceError::from(error)),
            }
        })
        .buffered(4);
    while let Some(catalog) = publications.next().await {
        if let Some(catalog) = catalog? {
            for reference in catalog.manifests {
                if reference.overlaps(start_ms, end_ms) {
                    references.insert(reference.index_key.clone(), reference);
                }
            }
        }
    }
    Ok((references, missing_summaries))
}
