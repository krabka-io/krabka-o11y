/// Synthetic label that names the active query shard (`N_of_M`).
///
/// Krabka's sharding is an internal scheme over the FNV
/// [`SeriesFingerprint`]. See [`QueryShardSelector::matches`]. It is
/// self-consistent but not byte-compatible with Mimir's stable label-hash
/// sharding, so this label is internal-only and must not cross the
/// Mimir-facing wire boundary.
pub const QUERY_SHARD_LABEL: &str = "__query_shard__";
