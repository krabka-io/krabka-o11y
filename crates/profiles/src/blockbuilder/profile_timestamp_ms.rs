use super::*;

#[must_use]
pub fn profile_timestamp_ms(timestamp_ns: i64) -> i64 {
    timestamp_ns.div_euclid(NANOS_PER_MILLI)
}
