use super::*;

pub(crate) fn metadata_series_from_v1(metadata: pb::v1::MetricMetadata) -> DecodedSeries {
    let mut labels = Labels::new();
    labels.insert("__name__", metadata.metric_family_name.as_str());
    DecodedSeries {
        labels,
        samples: Vec::new(),
        histograms: Vec::new(),
        exemplars: Vec::new(),
        metadata: Some(DecodedMetadata {
            metric_family_name: metadata.metric_family_name,
            metric_type: metadata_type(metadata.r#type),
            help: metadata.help,
            unit: metadata.unit,
        }),
    }
}
