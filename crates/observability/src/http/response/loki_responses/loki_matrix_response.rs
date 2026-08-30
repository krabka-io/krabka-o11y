use super::{FormattedMetricSeries, Value, loki_matrix_response_with_warnings};

pub(crate) fn loki_matrix_response(series: FormattedMetricSeries) -> Value {
    loki_matrix_response_with_warnings(series, &[])
}
