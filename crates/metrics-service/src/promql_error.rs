use super::MetricsServiceError;

impl From<MetricsServiceError> for krabka_promql::PromqlError {
    fn from(error: MetricsServiceError) -> Self {
        Self::Store(error.to_string())
    }
}
