use super::*;

/// A JSON push whose value is the empty string carries no timestamp of its
/// own, so it is refused outright against the stale-sample window rather
/// than dated to the epoch. With no window configured there is nothing to
/// refuse it against.
#[test]
pub(crate) fn an_empty_json_value_is_refused_only_when_a_stale_window_is_set() {
    use krabka_units::hours;

    let labels = Labels::default();
    check!(validate_loki_empty_json_value_timestamp_window(&labels, None).is_ok());

    let error = validate_loki_empty_json_value_timestamp_window(&labels, Some(hours(1)))
        .expect_err("a configured window refuses an undated sample");
    check!(matches!(
        error,
        DistributorError::TimestampTooOldString {
            timestamp: "0001-01-01T00:00:00Z",
            ..
        }
    ));
}
