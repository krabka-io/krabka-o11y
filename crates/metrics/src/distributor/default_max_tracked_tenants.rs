/// Default cap on the number of distinct tenants the distributor keeps
/// in-memory state for: the active-series tracker and the OTLP delta
/// accumulators.
///
/// Both maps are keyed by a tenant id that arrives in a request header, so an
/// unbounded set of ids would grow memory without limit. A misbehaving or
/// hostile client can send such a set. After this many tenants are tracked, the
/// least-recently-written tenant makes room for the new one. The value matches
/// `DEFAULT_MAX_RATE_BUCKETS`, which bounds the third per-tenant map.
pub const DEFAULT_MAX_TRACKED_TENANTS: usize = 100_000;
