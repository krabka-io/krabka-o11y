use super::{json, TsdbBlock, Value};

pub(crate) fn tsdb_blocks_json(mut blocks: Vec<TsdbBlock>) -> Vec<Value> {
    blocks.sort_by(|left, right| {
        left.min_time
            .cmp(&right.min_time)
            .then_with(|| left.max_time.cmp(&right.max_time))
            .then_with(|| left.id.cmp(&right.id))
    });
    blocks
        .into_iter()
        .map(|block| {
            json!({
                "ulid": block.id,
                "minTime": block.min_time,
                "maxTime": block.max_time,
                "stats": {
                    "numSamples": block.num_samples,
                    "numSeries": block.num_series,
                    "numChunks": block.num_series,
                },
            })
        })
        .collect()
}
