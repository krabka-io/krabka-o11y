use super::{BTreeMap, Labels};

pub(crate) struct LimitedVolumeSeries {
    pub(crate) labels: Labels,
    pub(crate) samples: BTreeMap<i64, u64>,
}

pub(crate) fn limit_volume_series(
    volumes: BTreeMap<Labels, BTreeMap<i64, u64>>,
    limit: usize,
) -> Vec<LimitedVolumeSeries> {
    volumes
        .into_iter()
        .take(limit)
        .map(|entry| LimitedVolumeSeries {
            labels: entry.0,
            samples: entry.1,
        })
        .collect()
}
