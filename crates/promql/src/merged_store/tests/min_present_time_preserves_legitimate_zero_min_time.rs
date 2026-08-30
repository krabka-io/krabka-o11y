#[test]
pub(crate) fn min_present_time_preserves_legitimate_zero_min_time() {
    // A store that holds samples whose earliest is epoch 0 must report 0,
    // not be treated as empty and discarded in favor of the other store.
    // Absent stores fall back to the present one; both-empty is 0.
    for (left, right, want) in [
        (Some(0), Some(50), 0),
        (Some(0), None, 0),
        (None, Some(0), 0),
        (None, Some(50), 50),
        (Some(50), None, 50),
        (None, None, 0),
        (Some(20), Some(50), 20),
    ] {
        assert2::assert!(super::super::min_present_time(left, right) == want);
    }
}
