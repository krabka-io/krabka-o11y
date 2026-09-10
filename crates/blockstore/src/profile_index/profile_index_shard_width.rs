/// Width of one profile-index shard, in the milliseconds the profiles path
/// counts.
///
/// A day, for the reason [`crate::DEFAULT_INDEX_SHARD_WIDTH`] gives and with
/// the same arithmetic: a narrower grid puts fewer records in the shard a
/// flush rewrites and more entries in the manifest every flush republishes.
///
/// The profiles block builder divides its nanosecond WAL timestamps down to
/// milliseconds before they reach the index, so this is the same span as the
/// traces path's shard and a different number.
pub(crate) const PROFILE_INDEX_SHARD_WIDTH: i64 = 24 * 60 * 60 * 1_000;
