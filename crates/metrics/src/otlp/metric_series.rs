use super::*;

pub(crate) fn metric_series(
    metric: &Metric,
    resource_attributes: &[KeyValue],
    strategy: TranslationStrategy,
    accumulator: Option<&mut DeltaAccumulator>,
) -> Result<Vec<DecodedSeries>, OtlpError> {
    let Some(data) = &metric.data else {
        return Ok(Vec::new());
    };

    reject_far_future_points(&metric.name, data)?;

    match data {
        metric::Data::Gauge(gauge) => gauge_series(metric, gauge, resource_attributes, strategy),
        metric::Data::Sum(sum) => {
            sum_series(metric, sum, resource_attributes, strategy, accumulator)
        }
        metric::Data::Histogram(histogram) => histogram_series(
            metric,
            histogram,
            resource_attributes,
            strategy,
            accumulator,
        ),
        metric::Data::ExponentialHistogram(histogram) => exponential_histogram_series(
            metric,
            histogram,
            resource_attributes,
            strategy,
            accumulator,
        ),
        metric::Data::Summary(summary) => Ok(summary_series(
            metric,
            summary,
            resource_attributes,
            strategy,
        )),
    }
}
