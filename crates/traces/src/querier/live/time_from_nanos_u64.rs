use super::*;

/// OTLP carries nanosecond fields as `uint64`. Saturate rather than wrap when
/// one exceeds what a `Time` extent can be built from.
pub(crate) fn time_from_nanos_u64(nanos: u64) -> Time {
    Time::from_nanos(i64::try_from(nanos).unwrap_or(i64::MAX))
}
