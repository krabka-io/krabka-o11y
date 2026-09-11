use krabka_metrics::LimitError;

use crate::PromqlError;

/// The error for a query that reads more samples than the tenant's cap allows.
///
/// `observed` is the sample count at the point the caller stops, which is the
/// first count above `limit`.
pub(crate) fn samples_per_query_exceeded(limit: usize, observed: usize) -> PromqlError {
    PromqlError::Limit(LimitError::SamplesPerQueryExceeded {
        limit: u64::try_from(limit).unwrap_or(u64::MAX),
        observed: u64::try_from(observed).unwrap_or(u64::MAX),
    })
}
