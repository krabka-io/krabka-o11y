use super::{
    DecodedSeries, DeltaAccumulator, HistogramScope, KeyValue, Metric, OtlpError,
    TranslationStrategy, exponential_histogram_series, gauge_series, histogram_series, metric,
    reject_far_future_points, sum_series, summary_series,
};

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
    let histogram_scope = HistogramScope {
        metric,
        resource_attributes,
        strategy,
    };

    match data {
        metric::Data::Gauge(gauge) => gauge_series(metric, gauge, resource_attributes, strategy),
        metric::Data::Sum(sum) => {
            sum_series(metric, sum, resource_attributes, strategy, accumulator)
        }
        metric::Data::Histogram(histogram) => {
            histogram_series(&histogram_scope, histogram, accumulator)
        }
        metric::Data::ExponentialHistogram(histogram) => {
            exponential_histogram_series(&histogram_scope, histogram, accumulator)
        }
        metric::Data::Summary(summary) => Ok(summary_series(
            metric,
            summary,
            resource_attributes,
            strategy,
        )),
    }
}
