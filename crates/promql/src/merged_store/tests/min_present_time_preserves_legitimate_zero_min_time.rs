#[yare::parameterized(
    epoch_beats_later = { Some(0), Some(50), 0 },
    epoch_beats_empty = { Some(0), None, 0 },
    epoch_from_right = { None, Some(0), 0 },
    right_fallback = { None, Some(50), 50 },
    left_fallback = { Some(50), None, 50 },
    both_empty = { None, None, 0 },
    earlier_present = { Some(20), Some(50), 20 },
)]
pub(crate) fn min_present_time_preserves_legitimate_zero_min_time(
    left: Option<i64>,
    right: Option<i64>,
    want: i64,
) {
    // A store that holds samples whose earliest is epoch 0 must report 0,
    // not be treated as empty and discarded in favor of the other store.
    // Absent stores fall back to the present one; both-empty is 0.
    assert2::assert!(crate::merged_store::min_present_time(left, right) == want);
}
