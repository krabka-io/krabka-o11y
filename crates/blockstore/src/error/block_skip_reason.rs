use super::Display;

/// Why a scan left one block out of the result it returned.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum BlockSkipReason {
    /// The index named a key the object store does not have.
    Missing,

    /// The object is there, and is not a readable Parquet block.
    Corrupt,
}

impl Display for BlockSkipReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let word = match self {
            Self::Missing => "missing",
            Self::Corrupt => "corrupt",
        };
        f.write_str(word)
    }
}
