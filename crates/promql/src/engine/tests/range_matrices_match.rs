use super::*;

/// Compares two `RangeMatrix` results for the range parity test.
///
/// The comparison is bit-exact on float sample values, so a genuine NaN equals
/// a genuine NaN. A plain `PartialEq` fails on `NaN == NaN`. Series order,
/// labelsets, per-step timestamps, and gaps must all match.
pub(crate) fn range_matrices_match(left: &QueryResult, right: &QueryResult) -> bool {
    let (QueryResult::RangeMatrix(left), QueryResult::RangeMatrix(right)) = (left, right) else {
        return false;
    };
    if left.len() != right.len() {
        return false;
    }
    left.iter().zip(right.iter()).all(|(l, r)| {
        l.labels == r.labels
            && l.samples.len() == r.samples.len()
            && l.samples.iter().zip(r.samples.iter()).all(|(lp, rp)| {
                lp.0 == rp.0
                    && match (&lp.1, &rp.1) {
                        (SampleValue::Float(a), SampleValue::Float(b)) => {
                            a.to_bits() == b.to_bits()
                        }
                        (a, b) => a == b,
                    }
            })
    })
}
