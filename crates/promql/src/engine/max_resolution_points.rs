/// Maximum number of resolution points (steps) in one range or subquery series.
/// Prometheus rejects a query whose `(end - start) / step + 1` is more than this
/// limit. The limit stops an abusive resolution, for example
/// `last_over_time(up[1000d:1ms])`, before the per-step loop runs.
pub const MAX_RESOLUTION_POINTS: u64 = 11_000;
