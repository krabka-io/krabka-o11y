use super::*;

pub(crate) fn resource_metrics_timestamp_ms(
    resource_metrics: &opentelemetry_proto::tonic::metrics::v1::ResourceMetrics,
) -> Option<i64> {
    for scope_metrics in &resource_metrics.scope_metrics {
        for metric in &scope_metrics.metrics {
            let Some(data) = &metric.data else {
                continue;
            };
            let timestamp = match data {
                metric::Data::Gauge(gauge) => {
                    gauge.data_points.first().map(|point| point.time_unix_nano)
                }
                metric::Data::Sum(sum) => sum.data_points.first().map(|point| point.time_unix_nano),
                metric::Data::Histogram(histogram) => histogram
                    .data_points
                    .first()
                    .map(|point| point.time_unix_nano),
                metric::Data::ExponentialHistogram(histogram) => histogram
                    .data_points
                    .first()
                    .map(|point| point.time_unix_nano),
                metric::Data::Summary(summary) => summary
                    .data_points
                    .first()
                    .map(|point| point.time_unix_nano),
            };
            if let Some(timestamp) = timestamp {
                return Some(nanos_to_millis(timestamp));
            }
        }
    }
    None
}
