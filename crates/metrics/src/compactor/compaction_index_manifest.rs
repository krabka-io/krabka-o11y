use super::{
    BlockMeta, CompactionIndexError, CompactionObjectPlan, CompactionSeriesLabels, Deserialize,
    MetricBlockKind, Serialize,
};

/// Compaction index sidecar written next to a metric block object.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CompactionIndexManifest {
    pub tenant: String,
    pub kind: MetricBlockKind,
    pub block_key: String,
    pub index_key: String,
    pub first_offset: i64,
    pub last_offset: i64,
    pub row_count: usize,
    pub min_ts: i64,
    pub max_ts: i64,
    pub fingerprints: Vec<u64>,
    pub series: Vec<CompactionSeriesLabels>,
}

impl CompactionIndexManifest {
    #[must_use]
    pub fn from_plan(
        tenant: impl Into<String>,
        kind: MetricBlockKind,
        plan: &CompactionObjectPlan,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            kind,
            block_key: plan.block_key.clone(),
            index_key: plan.index_key.clone(),
            first_offset: plan.first_offset,
            last_offset: plan.last_offset,
            row_count: plan.row_count,
            min_ts: 0,
            max_ts: 0,
            fingerprints: Vec::new(),
            series: Vec::new(),
        }
    }

    #[must_use]
    pub fn from_block_meta(
        kind: MetricBlockKind,
        plan: &CompactionObjectPlan,
        meta: &BlockMeta,
        series: Vec<CompactionSeriesLabels>,
    ) -> Self {
        Self {
            tenant: meta.tenant.clone(),
            kind,
            block_key: plan.block_key.clone(),
            index_key: plan.index_key.clone(),
            first_offset: plan.first_offset,
            last_offset: plan.last_offset,
            row_count: meta.row_count,
            min_ts: meta.min_ts,
            max_ts: meta.max_ts,
            fingerprints: meta.fingerprints.clone(),
            series,
        }
    }

    /// Encodes with `serde-wincode`, which matches the WAL record codec.
    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub fn encode(&self) -> Result<Vec<u8>, CompactionIndexError> {
        <serde_wincode::SerdeCompat<CompactionIndexManifest> as wincode::Serialize>::serialize(self)
            .map_err(|error| CompactionIndexError::Encode(error.to_string()))
    }

    /// Decodes a [`CompactionIndexManifest`] from its `serde-wincode` bytes.
    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub fn decode(bytes: &[u8]) -> Result<Self, CompactionIndexError> {
        <serde_wincode::SerdeCompat<CompactionIndexManifest> as wincode::Deserialize>::deserialize(
            bytes,
        )
        .map_err(|error| CompactionIndexError::Decode(error.to_string()))
    }
}
