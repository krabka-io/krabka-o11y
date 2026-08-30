use super::*;

/// The per-block trace footprint registered by a block builder.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TraceBlockStats {
    pub object_key: String,
    pub min_ts: i64,
    pub max_ts: i64,
    pub bloom: ShardedTraceBloom,
    pub tag_names: BTreeSet<String>,
    pub tag_values: BTreeMap<String, BTreeSet<String>>,
}
