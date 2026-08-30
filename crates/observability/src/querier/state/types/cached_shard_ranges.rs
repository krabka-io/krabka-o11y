use super::*;

#[derive(Clone)]
pub(crate) struct CachedShardRanges {
    pub(crate) loaded_at: Instant,
    pub(crate) listed_from_ns: i64,
    pub(crate) ranges: Vec<TimeRange>,
}
