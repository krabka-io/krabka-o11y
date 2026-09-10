use super::{
    BTreeMap, BTreeSet, BlockStoreError, IndexShardRange, ManifestShard, Result,
    SNAPSHOT_MANIFEST_VERSION, shard_payload_object_key,
};

/// What one generation of an index is made of.
///
/// This is the object the compare-and-swap swaps. It holds no index data of
/// its own: each entry names a shard payload, and the payloads are immutable
/// and content-addressed, so a generation that changes one shard writes one
/// new payload and republishes a manifest that names the rest by the keys the
/// previous generation already used.
///
/// Shards are grouped by tenant so that the tenant name is spelled once rather
/// than once per shard, and so that a load for one tenant can find its shards
/// without walking the fleet.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct SnapshotManifest {
    #[serde(rename = "v")]
    pub(crate) version: u32,
    #[serde(rename = "t")]
    pub(crate) tenants: BTreeMap<String, Vec<ManifestShard>>,
}

impl SnapshotManifest {
    pub(crate) fn new() -> Self {
        Self {
            version: SNAPSHOT_MANIFEST_VERSION,
            tenants: BTreeMap::new(),
        }
    }

    /// Parses a stored manifest and rejects one this build cannot read.
    ///
    /// `label` names the snapshot flavour in the error text.
    pub(crate) fn from_bytes(label: &str, bytes: &[u8]) -> Result<Self> {
        let manifest: Self = serde_json::from_slice(bytes)?;
        if manifest.version != SNAPSHOT_MANIFEST_VERSION {
            return Err(BlockStoreError::InvalidBlock(format!(
                "{label} is manifest version {}, expected {SNAPSHOT_MANIFEST_VERSION}",
                manifest.version
            )));
        }
        Ok(manifest)
    }

    /// # Errors
    /// Returns an error when the manifest cannot be serialised.
    pub(crate) fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    /// Records that `tenant` holds a shard of `range` with these payload bytes.
    pub(crate) fn insert(&mut self, tenant: &str, range: IndexShardRange, content: String) {
        self.tenants
            .entry(tenant.to_string())
            .or_default()
            .push(ManifestShard {
                start: range.start,
                end: range.end,
                content,
            });
    }

    /// Puts every tenant's shards in ascending span order, so that a manifest
    /// is a function of what it names and not of the order it was built in.
    pub(crate) fn sort(&mut self) {
        for shards in self.tenants.values_mut() {
            shards.sort_by(|left, right| {
                left.start
                    .cmp(&right.start)
                    .then_with(|| left.end.cmp(&right.end))
                    .then_with(|| left.content.cmp(&right.content))
            });
        }
        self.tenants.retain(|_, shards| !shards.is_empty());
    }

    pub(crate) fn shards_of(&self, tenant: &str) -> &[ManifestShard] {
        self.tenants.get(tenant).map_or(&[], Vec::as_slice)
    }

    /// Every tenant the manifest names, in name order.
    pub(crate) fn tenants(&self) -> impl Iterator<Item = &String> {
        self.tenants.keys()
    }

    /// Object keys of every payload this manifest names.
    pub(crate) fn payload_object_keys(&self, key: &str) -> BTreeSet<String> {
        self.tenants
            .iter()
            .flat_map(|(tenant, shards)| {
                shards.iter().map(move |shard| {
                    shard_payload_object_key(key, tenant, shard.range(), &shard.content)
                })
            })
            .collect()
    }
}
