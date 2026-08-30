use super::*;

pub(crate) fn loki_matrix_response_with_warnings(
    series: FormattedMetricSeries,
    warnings: &[String],
) -> Value {
    let result = series
        .into_iter()
        .map(|(metric, values)| {
            json!({
                "metric": metric,
                "values": values
                    .into_iter()
                    .map(loki_metric_sample)
                    .collect::<Vec<_>>(),
            })
        })
        .collect::<Vec<_>>();

    let mut value = loki_success_value(json!({
        "resultType": "matrix",
        "result": result,
    }));
    if !warnings.is_empty() {
        value["warnings"] = json!(warnings);
    }
    value
}
