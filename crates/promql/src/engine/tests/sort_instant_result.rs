use super::*;

/// Sorts an instant-vector result by fingerprint in place.
///
/// The function does nothing for the other result shapes. `query_results_match`
/// can then compare vectors independent of order.
pub(crate) fn sort_instant_result(result: QueryResult) -> QueryResult {
    match result {
        QueryResult::InstantVector(mut samples) => {
            samples.sort_by_key(|sample| sample.labels.fingerprint());
            QueryResult::InstantVector(samples)
        }
        other => other,
    }
}
