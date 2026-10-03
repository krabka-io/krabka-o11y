use super::{COMPACTION_OBJECT_PREFIX, CompactionIndexManifest, escape_object_path_segment};

/// Every `.index` manifest and publication marker under the metrics prefix.
///
/// See [`CompactionIndexListing`](super::CompactionIndexListing) for the rule
/// that makes a manifest live.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CompactionIndex {
    /// The manifests that queries read, in key order.
    pub live: Vec<CompactionIndexManifest>,
    /// The manifests of TSDB imports that are not published yet, in key
    /// order. Queries do not read them, and compaction and retention do not
    /// change them. The orphan sweep keeps them and their blocks, so that a
    /// retry of the import can publish them.
    pub pending: Vec<CompactionIndexManifest>,
    /// The keys of the publication markers of TSDB imports, in key order.
    /// The orphan sweep keeps every marker. A marker stays after compaction
    /// or retention removes the manifests of its import, so that a retry of
    /// the import does not publish the same samples again.
    pub markers: Vec<String>,
}

impl CompactionIndex {
    /// The keys of every manifest, block and marker of `tenant`, in key
    /// order.
    #[must_use]
    pub fn tenant_keys(&self, tenant: &str) -> Vec<String> {
        let directory = format!(
            "{COMPACTION_OBJECT_PREFIX}/{}/",
            escape_object_path_segment(tenant)
        );
        let mut keys: Vec<String> = self
            .live
            .iter()
            .chain(&self.pending)
            .filter(|manifest| manifest.tenant == tenant)
            .flat_map(|manifest| [manifest.index_key.clone(), manifest.block_key.clone()])
            .chain(
                self.markers
                    .iter()
                    .filter(|key| key.starts_with(&directory))
                    .cloned(),
            )
            .collect();
        keys.sort();
        keys.dedup();
        keys
    }
}
