use super::*;

pub(crate) fn limit_error_to_traces_error(err: &LimitError) -> TracesError {
    match err {
        LimitError::IngestionRateExceeded { .. } => TracesError::RateLimit(err.message()),
        LimitError::MaxSpansPerTrace { .. }
        | LimitError::AttributeTooLong { .. }
        | LimitError::TracesPerSearchExceeded { .. }
        | LimitError::SearchDurationExceeded { .. } => TracesError::Limit(err.message()),
    }
}
