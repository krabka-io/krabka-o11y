use super::BlockShape;

/// The part an object plays in its signal's layout.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObjectRole {
    /// A Parquet block.
    Block(BlockShape),
    /// A metrics `.index` sidecar. It is written after its block and it is
    /// what makes the block live.
    MetricsIndex { block: String },
    /// A profiles `.symdb` symbol table, written after its block.
    Symbols { block: String },
    /// A generation, payload or shard object of a snapshot index.
    IndexSnapshot,
    /// `tenant=…/index/logs/manifest.json`.
    LogTenantManifest,
    /// `tenant=…/index/logs/shards/manifest.json`.
    LogShardCatalog,
    /// A JSON generation under `tenant=…/index/logs/shards/time=S-E/manifest/snapshots`.
    LogShardManifest { start_ns: i64, end_ns: i64 },
    /// `tenant=…/index/logs/shards/time=N`, a listing aid with no payload.
    LogShardListOffset,
    /// `index/logs/manifest.json`.
    LogGlobalManifest,
    /// `index/logs/compaction-frontier.json`.
    LogFrontier,
    /// `mimir-tenant-deletions/{tenant}.json`.
    TenantDeletion,
    /// `metric-erasure-requests/{tenant}/{id}.json`.
    ErasureRequest { id: String },
    /// An object the audit knows and does not check: an upload in staging,
    /// or profiles debug info.
    Staging,
}
