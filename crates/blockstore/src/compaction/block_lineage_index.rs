use super::{BTreeMap, BTreeSet, BlockLevel, BlockLineage, Deserialize, Serialize};

/// Per-block levels and lineage, keyed by object key.
///
/// A signal index keeps one of these beside its own block records. Object keys
/// are unique across tenants, so the map is flat.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BlockLineageIndex {
    blocks: BTreeMap<String, BlockLineage>,
}

impl BlockLineageIndex {
    /// Registers a block written from ingested data at
    /// [`BlockLevel::INGESTED`].
    ///
    /// A block that already has lineage keeps it. Index snapshots are merged
    /// by re-adding every block, and a compacted block re-added that way must
    /// not be demoted to level zero.
    pub fn record_ingested(&mut self, object_key: &str, row_count: usize) {
        let lineage = self.blocks.entry(object_key.to_string()).or_default();
        if row_count > 0 {
            lineage.row_count = row_count;
        }
    }

    /// Registers a block that replaced `sources`, one level above the highest
    /// of them.
    ///
    /// A block that replaced nothing replaced no compacted block either, so it
    /// sits at [`BlockLevel::INGESTED`] rather than one rung above it.
    pub fn record_compacted(&mut self, object_key: &str, sources: &[String], row_count: usize) {
        let level = sources
            .iter()
            .map(|source| self.level(source))
            .max()
            .map_or(BlockLevel::INGESTED, BlockLevel::next);
        self.blocks.insert(
            object_key.to_string(),
            BlockLineage {
                level,
                row_count,
                sources: sources.to_vec(),
            },
        );
    }

    /// Drops the lineage of blocks that no longer exist.
    pub fn forget<'a, I>(&mut self, object_keys: I)
    where
        I: IntoIterator<Item = &'a str>,
    {
        for object_key in object_keys {
            self.blocks.remove(object_key);
        }
    }

    /// Drops every entry whose block is not in `live`, which is what keeps the
    /// map from growing once for every block ever written.
    pub fn retain_keys(&mut self, live: &BTreeSet<String>) {
        self.blocks
            .retain(|object_key, _| live.contains(object_key));
    }

    /// Folds `other` in, with `other` winning on any block both name.
    pub fn merge_from(&mut self, other: &Self) {
        for (object_key, lineage) in &other.blocks {
            self.blocks.insert(object_key.clone(), lineage.clone());
        }
    }

    /// The level of `object_key`, or [`BlockLevel::INGESTED`] for a block with
    /// no recorded lineage.
    #[must_use]
    pub fn level(&self, object_key: &str) -> BlockLevel {
        self.blocks
            .get(object_key)
            .map_or(BlockLevel::INGESTED, |lineage| lineage.level)
    }

    /// The recorded row count of `object_key`, or `0` when none was reported.
    #[must_use]
    pub fn row_count(&self, object_key: &str) -> usize {
        self.blocks
            .get(object_key)
            .map_or(0, |lineage| lineage.row_count)
    }

    /// The full lineage record of `object_key`, when it has one.
    #[must_use]
    pub fn lineage(&self, object_key: &str) -> Option<&BlockLineage> {
        self.blocks.get(object_key)
    }

    /// How many blocks have a lineage record.
    #[must_use]
    pub fn len(&self) -> usize {
        self.blocks.len()
    }

    /// Whether no block has a lineage record.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }
}
