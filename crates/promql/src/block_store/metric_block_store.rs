use super::{BlockStore, CompactionIndexManifest, MetricBlockKind, apply_manifest_to_blockstore};

/// `PromQL` metric store over compacted metric blocks.
#[derive(Clone)]
pub struct MetricBlockStore {
    pub(crate) floats: BlockStore,
    pub(crate) histograms: Option<BlockStore>,
    pub(crate) exemplars: Option<BlockStore>,
    pub(crate) metadata: Option<BlockStore>,
    pub(crate) metric_labels: std::collections::BTreeMap<(String, u64), crate::PromqlLabels>,
}

impl MetricBlockStore {
    #[must_use]
    pub fn new(float_store: BlockStore) -> Self {
        Self {
            floats: float_store,
            histograms: None,
            exemplars: None,
            metadata: None,
            metric_labels: std::collections::BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn with_histograms(float_store: BlockStore, histogram_store: BlockStore) -> Self {
        Self {
            floats: float_store,
            histograms: Some(histogram_store),
            exemplars: None,
            metadata: None,
            metric_labels: std::collections::BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn from_compaction_manifests(
        float_store: BlockStore,
        histogram_store: Option<BlockStore>,
        manifests: &[CompactionIndexManifest],
    ) -> Self {
        Self::from_compaction_manifest_refs(float_store, histogram_store, manifests)
    }

    #[must_use]
    pub fn from_compaction_manifest_refs<'a>(
        mut float_store: BlockStore,
        histogram_store: Option<BlockStore>,
        manifests: impl IntoIterator<Item = &'a CompactionIndexManifest>,
    ) -> Self {
        let mut histograms = histogram_store;
        let mut exemplars = None::<BlockStore>;
        let mut metadata = None::<BlockStore>;
        let mut metric_labels = std::collections::BTreeMap::new();
        for manifest in manifests {
            for series in &manifest.series {
                if series.labels.has_byte_values() {
                    metric_labels.insert(
                        (manifest.tenant.clone(), series.fingerprint),
                        series.labels.clone(),
                    );
                }
            }
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
            metric_labels,
        }
    }
}
