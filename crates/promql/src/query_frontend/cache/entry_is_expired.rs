use super::*;

/// Returns `true` if `inserted_epoch_millis` is older than `ttl`.
///
/// The age is measured against `now_epoch_millis`. A `None` TTL never expires.
pub(crate) fn entry_is_expired(ttl: Option<Time>, inserted_epoch_millis: i64, now_epoch_millis: i64) -> bool {
    let Some(ttl) = ttl else {
        return false;
    };
    let age = Time::from_millis(now_epoch_millis.saturating_sub(inserted_epoch_millis));
    age > ttl
}
