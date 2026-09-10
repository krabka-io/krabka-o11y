/// Width of one trace-index shard, in the nanoseconds the traces path counts.
///
/// A day. The choice trades two costs against each other. A narrower grid puts
/// fewer block records in the shard a flush rewrites, and more entries in the
/// manifest that every flush republishes; a wider one does the reverse. At a
/// day, a tenant retaining a month of traces has about thirty manifest entries,
/// which is under two kilobytes, and a flush rewrites the records of one day
/// rather than of the retention.
///
/// Unlike [`crate::DEFAULT_INDEX_SHARD_WIDTH`], this is not a guess that
/// widens if it turns out wrong: the traces path counts nanoseconds and
/// nothing else does. A record whose span is wider than
/// [`crate::index_snapshot`] allows still leaves the grid, which is what
/// bounds a tenant's shard count when a clock is wrong.
pub(crate) const TRACE_INDEX_SHARD_WIDTH: i64 = 24 * 60 * 60 * 1_000_000_000;
