use super::{TsdbBlock, Value, json};
use crate::store::sort_tsdb_blocks;

pub(crate) fn tsdb_blocks_json(mut blocks: Vec<TsdbBlock>) -> Vec<Value> {
    sort_tsdb_blocks(&mut blocks);
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
