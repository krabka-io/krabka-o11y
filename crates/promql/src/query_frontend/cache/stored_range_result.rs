use super::*;

/// Cached object-store payload: the annotated range result and its store timestamp.
///
/// The timestamp is the wall-clock instant of the store operation. A reader
/// enforces a TTL from it and does not depend on the object-store
/// `last_modified` metadata.
///
/// The payload holds the annotations of the evaluation, so a hit on this object
/// reports the same warnings and infos as the evaluation that stored it.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct StoredRangeResult {
    pub(crate) stored_at_ms: i64,
    pub(crate) result: AnnotatedQueryResult,
}
