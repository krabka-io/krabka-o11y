use super::LifecycleError;

/// Errors raised while reconciling the span block prefix against the index.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BlockSweepError {
    /// The trace index is stored inside the prefix the sweep would reconcile.
    ///
    /// The sweep deletes every object under its prefix that the index does not
    /// name, and the index does not name its own snapshots, shard manifests or
    /// shard payloads. A sweep over a prefix that holds them deletes the index
    /// itself. Every block then stays in the bucket, unreferenced and
    /// unqueryable, with nothing to report the loss, and the next sweep deletes
    /// the blocks as well because nothing names them. The sweep refuses
    /// instead. An operator should move `--trace-index-key` outside the block
    /// prefix.
    #[error(
        "the trace index key `{trace_index_key}` is inside the swept block prefix `{prefix}`: \
         move the index key outside it"
    )]
    IndexInsideBlockPrefix {
        prefix: String,
        trace_index_key: String,
    },
    /// Listing the prefix failed, so the sweep never learned what exists.
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError),
}
