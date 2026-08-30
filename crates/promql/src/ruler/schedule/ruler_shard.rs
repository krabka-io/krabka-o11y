use super::{PromqlError, stable_hash_parts};

/// One ruler shard for deterministic rule-group ownership.
///
/// Shards are one-based to match Mimir's shard notation: `1_of_3`, `2_of_3`, ...
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RulerShard {
    pub index: usize,
    pub total: usize,
}

impl RulerShard {
    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub fn new(index: usize, total: usize) -> Result<Self, PromqlError> {
        if total == 0 {
            return Err(PromqlError::Plan(
                "ruler shard total must be positive".into(),
            ));
        }
        if index == 0 || index > total {
            return Err(PromqlError::Plan(format!(
                "ruler shard index must be between 1 and {total}"
            )));
        }
        Ok(Self { index, total })
    }

    #[must_use]
    pub fn owns_group(self, tenant: &str, namespace: &str, group_name: &str) -> bool {
        let buckets = self.total as u64;
        let shard_index =
            usize::try_from(stable_hash_parts(&[tenant, namespace, group_name]) % buckets)
                .unwrap_or(0);
        shard_index == self.index - 1
    }
}
