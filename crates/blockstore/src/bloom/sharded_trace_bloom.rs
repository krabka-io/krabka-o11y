use super::*;

/// Sharded trace-id bloom filter.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShardedTraceBloom {
    pub(crate) shards: Vec<BloomShard>,
}

impl ShardedTraceBloom {
    #[must_use]
    pub fn new(shard_count: usize, expected_items_per_shard: usize, fp_rate: f64) -> Self {
        let shard_count = shard_count.max(1);
        Self {
            shards: (0..shard_count)
                .map(|_| BloomShard::new(expected_items_per_shard, fp_rate))
                .collect(),
        }
    }

    #[must_use]
    pub fn with_tempo_defaults(expected_items: usize) -> Self {
        const ITEMS_PER_100_KIB_SHARD: usize = 85_000;
        let shard_count = expected_items.div_ceil(ITEMS_PER_100_KIB_SHARD).max(1);
        let per_shard = expected_items.div_ceil(shard_count).max(1);
        Self::new(shard_count, per_shard, 0.01)
    }

    #[must_use]
    pub fn match_all_with_tempo_defaults() -> Self {
        let mut bloom = Self::with_tempo_defaults(1);
        for shard in &mut bloom.shards {
            shard.bits.fill(u64::MAX);
        }
        bloom
    }

    /// Validates the invariants that constructors enforce but `Deserialize`
    /// bypasses, so a corrupt snapshot errors at load time and does not panic
    /// on the first lookup.
    ///
    /// The method rejects an empty `shards` vector, which would divide by zero
    /// in [`Self::shard_of`], and any structurally invalid shard.
    ///
    /// # Errors
    ///
    /// Returns a message describing the first invariant violated.
    pub fn validate(&self) -> Result<(), String> {
        if self.shards.is_empty() {
            return Err("bloom must have at least one shard".into());
        }
        for shard in &self.shards {
            shard.validate()?;
        }
        Ok(())
    }

    #[must_use]
    pub fn shard_of(&self, trace_id: &[u8; 16]) -> usize {
        (fnv1_32(trace_id) as usize) % self.shards.len()
    }

    pub fn insert(&mut self, trace_id: &[u8; 16]) {
        let shard = self.shard_of(trace_id);
        self.shards[shard].insert(trace_id);
    }

    #[must_use]
    pub fn maybe_contains(&self, trace_id: &[u8; 16]) -> bool {
        let shard = self.shard_of(trace_id);
        self.shards[shard].maybe_contains(trace_id)
    }
}
