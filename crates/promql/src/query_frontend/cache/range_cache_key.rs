use super::{FrontendRangeQuery, QueryShard, TimeExt};

/// The identity of one cached sub-range result.
///
/// The step stays a raw millisecond integer here. The key is a `BTreeMap` key
/// and an object-store path component. Both need the `Ord`/`Eq` that a
/// `f64`-backed [`Time`](krabka_units::Time) cannot supply.
/// [`RangeCacheKey::new`] does the conversion.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct RangeCacheKey {
    pub(crate) tenant: String,
    pub(crate) query: String,
    pub(crate) start_ms: i64,
    pub(crate) end_ms: i64,
    pub(crate) step_ms: i64,
    pub(crate) shard: Option<QueryShard>,
}

impl RangeCacheKey {
    pub(crate) fn new(tenant: &str, query: &FrontendRangeQuery) -> Self {
        Self {
            tenant: tenant.to_string(),
            query: query.query.clone(),
            start_ms: query.start_ms,
            end_ms: query.end_ms,
            step_ms: query.step.millis_i64(),
            shard: query.shard,
        }
    }
}
