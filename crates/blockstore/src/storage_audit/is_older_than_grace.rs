use super::{ObjectMeta, SystemTime, Time, TimeExt, UNIX_EPOCH};

/// Whether `meta` was last written at or before `now - grace`.
///
/// The cutoff matches [`reconcile_orphans`](crate::reconcile_orphans):
/// object timestamps are whole seconds, and a zero grace means every object
/// is old.
pub fn is_older_than_grace(meta: &ObjectMeta, grace: Time, now: SystemTime) -> bool {
    now.checked_sub(grace.to_std())
        .and_then(|at| at.duration_since(UNIX_EPOCH).ok())
        .and_then(|since| i64::try_from(since.as_secs()).ok())
        .is_some_and(|cutoff| meta.last_modified.timestamp() <= cutoff)
}
