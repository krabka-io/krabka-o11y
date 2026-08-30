use super::{BlockMeta, fnv1a};

pub(crate) fn compacted_key(tenant: &str, blocks: &[BlockMeta], input_keys: &[String]) -> String {
    let min_ts = blocks
        .iter()
        .map(|block| block.min_ts)
        .min()
        .unwrap_or_default();
    let max_ts = blocks
        .iter()
        .map(|block| block.max_ts)
        .max()
        .unwrap_or_default();
    format!(
        "blocks/{tenant}/compacted/{min_ts}-{max_ts}-{:016x}.parquet",
        fnv1a(input_keys)
    )
}
