use super::*;

/// Cached object-store payload: the range result and its store timestamp.
///
/// The timestamp is the wall-clock instant of the store operation. A reader
/// enforces a TTL from it and does not depend on the object-store
/// `last_modified` metadata.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct StoredRangeResult {
    pub(crate) stored_at_ms: i64,
    pub(crate) result: QueryResult,
}
