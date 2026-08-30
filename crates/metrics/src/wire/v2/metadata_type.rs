use super::pb;

pub(crate) fn metadata_type(value: i32) -> String {
    match pb::v2::metadata::MetricType::try_from(value) {
        Ok(pb::v2::metadata::MetricType::Counter) => "counter",
        Ok(pb::v2::metadata::MetricType::Gauge) => "gauge",
        Ok(pb::v2::metadata::MetricType::Histogram) => "histogram",
        Ok(pb::v2::metadata::MetricType::Gaugehistogram) => "gaugehistogram",
        Ok(pb::v2::metadata::MetricType::Summary) => "summary",
        Ok(pb::v2::metadata::MetricType::Info) => "info",
        Ok(pb::v2::metadata::MetricType::Stateset) => "stateset",
        Ok(pb::v2::metadata::MetricType::Unspecified) | Err(_) => "unknown",
    }
    .to_string()
}
