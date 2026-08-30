use super::*;

/// Compares two sorted instant-sample vectors bit-exactly for the parity tests.
///
/// A bit-exact float comparison makes a genuine NaN equal a genuine NaN.
/// `PartialEq` on `SampleValue::Float` uses IEEE `==`, and `NaN != NaN` under
/// that rule, so a plain `assert_eq!` fails when a path correctly keeps a
/// genuine NaN value. The tests do not expect stale-NaN markers to survive
/// selection on either path, so the markers never reach this comparison.
pub(crate) fn instant_samples_match(left: &[crate::InstantSample], right: &[crate::InstantSample]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter().zip(right.iter()).all(|(l, r)| {
        l.labels == r.labels
            && l.ts_ms == r.ts_ms
            && match (&l.value, &r.value) {
                (SampleValue::Float(a), SampleValue::Float(b)) => a.to_bits() == b.to_bits(),
                (other_l, other_r) => other_l == other_r,
            }
    })
}
