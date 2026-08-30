use super::*;

pub(crate) fn limit_volume_series(
    volumes: BTreeMap<Labels, BTreeMap<i64, u64>>,
    limit: usize,
) -> Vec<(Labels, BTreeMap<i64, u64>)> {
    volumes.into_iter().take(limit).collect()
}
