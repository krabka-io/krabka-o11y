use super::rendezvous_pick;

/// The ready querier that owns `key`, for work that is not shard-planned.
///
/// A `TraceQL`-metrics query runs as one unsharded job, so it has no shard to
/// assign; it still needs a target, and it still must not be a querier the
/// membership has ejected. Keying on the query text keeps a repeated query on
/// the same querier while the pool holds, which is worth having for whatever
/// that querier has already decoded.
#[must_use]
pub fn pick_querier<'a>(ready: &[&'a str], key: &str) -> Option<&'a str> {
    rendezvous_pick(ready, key)
}
