/// One chunk that the index lists for a series.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChunkMeta {
    pub min_time: i64,
    pub max_time: i64,
    pub reference: u64,
}
