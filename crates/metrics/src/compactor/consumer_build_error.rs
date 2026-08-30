use super::*;

pub(crate) fn consumer_build_error(error: &ConsumerError) -> MetricsCompactorBuildError {
    MetricsCompactorBuildError::Consumer(error.to_string())
}
