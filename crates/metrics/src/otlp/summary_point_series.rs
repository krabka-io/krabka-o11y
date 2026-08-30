use super::{DecodedMetadata, DecodedSample, DecodedSeries, KeyValue, SummaryDataPoint, ToPrimitive, labels, nanos_to_millis};

pub(crate) fn summary_point_series(
    name: &str,
    point: &SummaryDataPoint,
    resource_attributes: &[KeyValue],
    metadata: Option<DecodedMetadata>,
) -> Vec<DecodedSeries> {
    let timestamp = nanos_to_millis(point.time_unix_nano);
    let mut out = Vec::new();
    for quantile in &point.quantile_values {
        let quantile_value = quantile.quantile.to_string();
        out.push(DecodedSeries {
            labels: labels(
                name,
                resource_attributes,
                &point.attributes,
                Some(("quantile", &quantile_value)),
            ),
            samples: vec![DecodedSample::with_start_timestamp(
                timestamp,
                quantile.value,
                (point.start_time_unix_nano != 0)
                    .then_some(nanos_to_millis(point.start_time_unix_nano)),
            )],
            histograms: Vec::new(),
            exemplars: Vec::new(),
            metadata: metadata.clone(),
        });
    }
    out.push(DecodedSeries {
        labels: labels(
            &format!("{name}_count"),
            resource_attributes,
            &point.attributes,
            None,
        ),
        samples: vec![DecodedSample::with_start_timestamp(
            timestamp,
            point.count.to_f64().unwrap_or(f64::MAX),
            (point.start_time_unix_nano != 0)
                .then_some(nanos_to_millis(point.start_time_unix_nano)),
        )],
        histograms: Vec::new(),
        exemplars: Vec::new(),
        metadata: metadata.clone(),
    });
    out.push(DecodedSeries {
        labels: labels(
            &format!("{name}_sum"),
            resource_attributes,
            &point.attributes,
            None,
        ),
        samples: vec![DecodedSample::with_start_timestamp(
            timestamp,
            point.sum,
            (point.start_time_unix_nano != 0)
                .then_some(nanos_to_millis(point.start_time_unix_nano)),
        )],
        histograms: Vec::new(),
        exemplars: Vec::new(),
        metadata,
    });
    out
}
