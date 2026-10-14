/// Inclusive range of WAL offsets represented by a block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OffsetRange {
    pub min: i64,
    pub max: i64,
}

impl OffsetRange {
    #[must_use]
    pub const fn new(min: i64, max: i64) -> Self {
        Self { min, max }
    }
}
