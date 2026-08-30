use super::*;

pub(crate) fn apply_result_limit(result: &mut QueryResult, limit: Option<usize>) {
    match result {
        QueryResult::InstantVector(samples) => apply_limit(samples, limit),
        QueryResult::RangeMatrix(series) => apply_limit(series, limit),
        QueryResult::Scalar { .. } | QueryResult::Str { .. } => {}
    }
}
