use super::*;

/// Compares two whole [`QueryResult`]s for the parity tests below.
///
/// The comparison is NaN-aware across the scalar, vector, matrix, and string
/// shapes, so a genuine NaN equals a genuine NaN. The caller pre-sorts vectors
/// by fingerprint.
pub(crate) fn query_results_match(left: &QueryResult, right: &QueryResult) -> bool {
    match left {
        QueryResult::Scalar { ts_ms, value } => match right {
            QueryResult::Scalar {
                ts_ms: other_ts_ms,
                value: other_value,
            } => ts_ms == other_ts_ms && value.to_bits() == other_value.to_bits(),
            _ => false,
        },
        QueryResult::InstantVector(samples) => match right {
            QueryResult::InstantVector(other_samples) => {
                instant_samples_match(samples, other_samples)
            }
            _ => false,
        },
        QueryResult::RangeMatrix(_) => {
            matches!(right, QueryResult::RangeMatrix(_)) && range_matrices_match(left, right)
        }
        QueryResult::Str { ts_ms, value } => match right {
            QueryResult::Str {
                ts_ms: other_ts_ms,
                value: other_value,
            } => ts_ms == other_ts_ms && value == other_value,
            _ => false,
        },
    }
}
