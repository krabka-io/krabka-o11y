use krabka_metrics::LimitError;

use crate::PromqlError;

/// Error for a query that reads more samples than the tenant's cap allows.
pub(crate) fn samples_per_query_exceeded(limit: usize, observed: usize) -> PromqlError {
    PromqlError::Limit(LimitError::SamplesPerQueryExceeded {
        limit: u64::try_from(limit).unwrap_or(u64::MAX),
        observed: u64::try_from(observed).unwrap_or(u64::MAX),
    })
}

/// Error for a query that selects more series than the tenant's cap allows.
pub(crate) fn series_per_query_exceeded(limit: usize, observed: usize) -> PromqlError {
    PromqlError::Limit(LimitError::SeriesPerQueryExceeded {
        limit: u64::try_from(limit).unwrap_or(u64::MAX),
        observed: u64::try_from(observed).unwrap_or(u64::MAX),
    })
}
