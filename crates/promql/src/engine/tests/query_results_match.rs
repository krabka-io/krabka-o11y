use super::*;

/// Compares two whole [`QueryResult`]s for the parity tests below.
///
/// The comparison is NaN-aware across the scalar, vector, matrix, and string
/// shapes, so a genuine NaN equals a genuine NaN. The caller pre-sorts vectors
/// by fingerprint.
pub(crate) fn query_results_match(left: &QueryResult, right: &QueryResult) -> bool {
    match (left, right) {
        (
            QueryResult::Scalar {
                ts_ms: lt,
                value: lv,
            },
            QueryResult::Scalar {
                ts_ms: rt,
                value: rv,
            },
        ) => lt == rt && lv.to_bits() == rv.to_bits(),
        (QueryResult::InstantVector(left), QueryResult::InstantVector(right)) => {
            instant_samples_match(left, right)
        }
        (QueryResult::RangeMatrix(_), QueryResult::RangeMatrix(_)) => {
            range_matrices_match(left, right)
        }
        (
            QueryResult::Str {
                ts_ms: lt,
                value: lv,
            },
            QueryResult::Str {
                ts_ms: rt,
                value: rv,
            },
        ) => lt == rt && lv == rv,
        _ => false,
    }
}
