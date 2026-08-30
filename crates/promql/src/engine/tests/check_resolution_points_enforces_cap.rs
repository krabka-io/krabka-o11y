use super::*;

/// C2: `check_resolution_points` guards the step and the point count.
///
/// The function rejects a non-positive step. It rejects an abusive point count
/// above `MAX_RESOLUTION_POINTS`, and it accepts a count at the cap.
#[test]
pub(crate) fn check_resolution_points_enforces_cap() {
    // A non-positive step is rejected outright.
    assert2::assert!(check_resolution_points(0, 1_000, Time::ZERO).is_err());
    assert2::assert!(check_resolution_points(0, 1_000, millis(1) * -1.0).is_err());

    // `(end-start)/step == MAX_RESOLUTION_POINTS` intervals is accepted — the
    // same boundary the HTTP gate and Prometheus' `(end-start)/step > 11000`
    // rule admit (no off-by-one re-rejection of a gate-admitted query).
    let at_cap = i64::try_from(MAX_RESOLUTION_POINTS).unwrap(); // step = 1ms => intervals == MAX.
    assert2::assert!(check_resolution_points(0, at_cap, millis(1)).is_ok());

    // One interval over the cap errors.
    assert2::assert!(check_resolution_points(0, at_cap + 1, millis(1)).is_err());

    // The abusive `[1000d:1ms]`-style resolution is rejected before looping.
    let thousand_days_ms = 1_000_i64 * 24 * 60 * 60 * 1_000;
    let err = check_resolution_points(0, thousand_days_ms, millis(1)).expect_err("must reject");
    assert2::assert!(err.to_string().contains("exceeded maximum resolution"));
}
