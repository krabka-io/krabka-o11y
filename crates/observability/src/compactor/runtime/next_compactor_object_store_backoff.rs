use super::Time;

/// Doubles the object-store retry backoff, up to a cap.
///
/// `Time` is `PartialOrd` but not `Ord`, so this uses `Time::min` and not
/// `std::cmp::min`.
pub(crate) fn next_compactor_object_store_backoff(current: Time, max_backoff: Time) -> Time {
    (current * 2.0).min(max_backoff)
}
