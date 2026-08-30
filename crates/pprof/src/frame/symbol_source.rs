use super::Frame;

/// Resolves a raw `(partition, stacktrace_id)` into frames.
pub trait SymbolSource: Send + Sync {
    fn resolve(&self, partition: u64, id: u32) -> Vec<Frame>;
}
