use super::pb;

pub(crate) fn metadata_type(value: i32) -> String {
    match pb::v1::metric_metadata::MetricType::try_from(value) {
        Ok(pb::v1::metric_metadata::MetricType::Counter) => "counter",
        Ok(pb::v1::metric_metadata::MetricType::Gauge) => "gauge",
        Ok(pb::v1::metric_metadata::MetricType::Histogram) => "histogram",
        Ok(pb::v1::metric_metadata::MetricType::Gaugehistogram) => "gaugehistogram",
        Ok(pb::v1::metric_metadata::MetricType::Summary) => "summary",
        Ok(pb::v1::metric_metadata::MetricType::Info) => "info",
        Ok(pb::v1::metric_metadata::MetricType::Stateset) => "stateset",
        Ok(pb::v1::metric_metadata::MetricType::Unknown) | Err(_) => "unknown",
    }
    .to_string()
}
