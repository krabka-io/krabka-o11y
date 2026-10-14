/// One compacted TSDB block exposed by `/api/v1/status/tsdb/blocks`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TsdbBlock {
    pub id: String,
    pub min_time: i64,
    pub max_time: i64,
    pub num_samples: usize,
    pub num_series: usize,
}

pub(crate) fn sort_tsdb_blocks(blocks: &mut [TsdbBlock]) {
    blocks.sort_by(|left, right| {
        left.min_time
            .cmp(&right.min_time)
            .then_with(|| left.max_time.cmp(&right.max_time))
            .then_with(|| left.id.cmp(&right.id))
    });
}
