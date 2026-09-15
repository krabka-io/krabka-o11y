use super::{Frame, ResolvedLocation};

/// Resolves a raw `(partition, stacktrace_id)` into frames.
pub trait SymbolSource: Send + Sync {
    fn resolve(&self, partition: u64, id: u32) -> Vec<Frame>;

    fn resolve_locations(&self, partition: u64, id: u32) -> Vec<ResolvedLocation> {
        self.resolve(partition, id)
            .into_iter()
            .map(ResolvedLocation::from)
            .collect()
    }
}
