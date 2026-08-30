/// Default cap on the number of distinct tenants tracked for per-tenant
/// ingestion-rate token buckets. The map is otherwise insert-only, so an
/// unbounded set of tenant strings would grow memory without limit. A
/// misbehaving or hostile client can send such a set. After this many tenants
/// are tracked, the enforcer evicts the least-recently-touched bucket to make
/// room.
pub const DEFAULT_MAX_RATE_BUCKETS: usize = 100_000;
