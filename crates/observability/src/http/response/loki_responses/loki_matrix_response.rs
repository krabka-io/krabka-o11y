use super::*;

pub(crate) fn loki_matrix_response(series: FormattedMetricSeries) -> Value {
    loki_matrix_response_with_warnings(series, &[])
}
