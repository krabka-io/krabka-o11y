use super::{DecodedMetadata, Metric};

pub(crate) fn metric_metadata(
    metric: &Metric,
    metric_family_name: &str,
    metric_type: &str,
) -> DecodedMetadata {
    DecodedMetadata {
        metric_family_name: metric_family_name.to_string(),
        metric_type: metric_type.to_string(),
        help: metric.description.clone(),
        unit: metric.unit.clone(),
    }
}
