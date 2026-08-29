//! `MetricStore` adapter backed by `krabka-blockstore`.

use krabka_blockstore::{BlockMeta, BlockStore};
use krabka_metrics::{CompactionIndexManifest, MetricBlockKind};

mod store_impl;

/// `PromQL` metric store over compacted metric blocks.
#[derive(Clone)]
pub struct MetricBlockStore {
    floats: BlockStore,
    histograms: Option<BlockStore>,
    exemplars: Option<BlockStore>,
    metadata: Option<BlockStore>,
}

impl MetricBlockStore {
    #[must_use]
    pub fn new(float_store: BlockStore) -> Self {
        Self {
            floats: float_store,
            histograms: None,
            exemplars: None,
            metadata: None,
        }
    }

    #[must_use]
    pub fn with_histograms(float_store: BlockStore, histogram_store: BlockStore) -> Self {
        Self {
            floats: float_store,
            histograms: Some(histogram_store),
            exemplars: None,
            metadata: None,
        }
    }

    #[must_use]
    pub fn from_compaction_manifests(
        mut float_store: BlockStore,
        histogram_store: Option<BlockStore>,
        manifests: &[CompactionIndexManifest],
    ) -> Self {
        let mut histograms = histogram_store;
        let mut exemplars = None::<BlockStore>;
        let mut metadata = None::<BlockStore>;
        for manifest in manifests {
            match manifest.kind {
                MetricBlockKind::Float => apply_manifest_to_blockstore(&mut float_store, manifest),
                MetricBlockKind::NativeHistograms => {
                    if let Some(store) = &mut histograms {
                        apply_manifest_to_blockstore(store, manifest);
                    }
                }
                MetricBlockKind::Exemplars => {
                    let store = exemplars.get_or_insert_with(|| float_store.empty_like());
                    apply_manifest_to_blockstore(store, manifest);
                }
                MetricBlockKind::Metadata => {
                    let store = metadata.get_or_insert_with(|| float_store.empty_like());
                    apply_manifest_to_blockstore(store, manifest);
                }
                // A clock block is the source of truth for a clock reading, and
                // it holds the interval, the sync state and the reference
                // identity together in one row. `PromQL` reads the projection
                // the distributor writes beside it, which arrives here as
                // ordinary float samples, so this store registers no clock
                // block.
                MetricBlockKind::ClockReadings => {}
            }
        }
        Self {
            floats: float_store,
            histograms,
            exemplars,
            metadata,
        }
    }
}

fn apply_manifest_to_blockstore(store: &mut BlockStore, manifest: &CompactionIndexManifest) {
    for series in &manifest.series {
        store
            .index_mut()
            .add_series(&manifest.tenant, series.fingerprint, &series.labels);
    }
    store.index_mut().add_block(&BlockMeta {
        tenant: manifest.tenant.clone(),
        object_key: manifest.block_key.clone(),
        min_ts: manifest.min_ts,
        max_ts: manifest.max_ts,
        row_count: manifest.row_count,
        fingerprints: manifest.fingerprints.clone(),
    });
}

#[cfg(test)]
mod tests;
