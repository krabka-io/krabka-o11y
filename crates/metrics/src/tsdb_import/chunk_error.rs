/// Why one chunk's data did not decode.
///
/// The caller knows which series the chunk belongs to and turns this into a
/// [`TsdbImportError`](super::TsdbImportError) that names it.
#[derive(Debug, PartialEq)]
pub enum ChunkError {
    Truncated,
    Invalid(String),
    Schema(i64),
    TooManyBuckets(u64),
}

impl std::fmt::Display for ChunkError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => {
                formatter.write_str("chunk data ends early or holds an invalid bit sequence")
            }
            Self::Invalid(reason) => formatter.write_str(reason),
            Self::Schema(schema) => write!(formatter, "histogram schema {schema}"),
            Self::TooManyBuckets(count) => write!(formatter, "{count} histogram buckets"),
        }
    }
}
