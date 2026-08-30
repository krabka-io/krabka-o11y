use super::*;

#[test]
pub(crate) fn missing_timestamp_fallback_age_is_exact() {
    check!(LOKI_REJECT_OLD_SAMPLES_MAX_AGE == hours(168));
}
