use super::ChunkMeta;

/// One series entry of a TSDB index.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexSeries {
    /// The series reference: the entry's byte offset divided by 16. Postings
    /// and tombstones name a series by it.
    pub reference: u64,
    /// The label pairs, sorted by name.
    pub labels: Vec<(String, String)>,
    pub chunks: Vec<ChunkMeta>,
}
