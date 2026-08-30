use super::{
    DecodedMetadata, DecodedSample, DecodedSeries, HistogramDataPoint, KeyValue, OtlpError,
    ToPrimitive, exemplars_for_bucket, exemplars_from_histogram_point, labels, nanos_to_millis,
};

pub(crate) fn classic_histogram_series(
    name: &str,
    point: &HistogramDataPoint,
    resource_attributes: &[KeyValue],
    metadata: Option<&DecodedMetadata>,
) -> Result<Vec<DecodedSeries>, OtlpError> {
    if !point.bucket_counts.is_empty()
        && point.bucket_counts.len() != point.explicit_bounds.len() + 1
    {
        return Err(OtlpError::Invalid(
            name.into(),
            "bucket_counts length must be explicit_bounds length plus one".into(),
        ));
    }

    let timestamp = nanos_to_millis(point.time_unix_nano);
    let point_exemplars = exemplars_from_histogram_point(point);
    let mut out = Vec::new();
    let base_name = format!("{name}_bucket");
    let mut cumulative = 0_u64;
    for (idx, count) in point.bucket_counts.iter().enumerate() {
        cumulative = cumulative.saturating_add(*count);
        let le = point
            .explicit_bounds
            .get(idx)
            .map_or_else(|| "+Inf".to_string(), ToString::to_string);
        out.push(DecodedSeries {
            labels: labels(
                &base_name,
                resource_attributes,
                &point.attributes,
                Some(("le", &le)),
            ),
            samples: vec![DecodedSample::with_start_timestamp(
                timestamp,
                cumulative.to_f64().unwrap_or(f64::MAX),
                (point.start_time_unix_nano != 0)
                    .then_some(nanos_to_millis(point.start_time_unix_nano)),
            )],
            histograms: Vec::new(),
            exemplars: exemplars_for_bucket(&point_exemplars, point, idx),
            metadata: metadata.cloned(),
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
        metadata: metadata.cloned(),
    });
    if let Some(sum) = point.sum {
        out.push(DecodedSeries {
            labels: labels(
                &format!("{name}_sum"),
                resource_attributes,
                &point.attributes,
                None,
            ),
            samples: vec![DecodedSample::with_start_timestamp(
                timestamp,
                sum,
                (point.start_time_unix_nano != 0)
                    .then_some(nanos_to_millis(point.start_time_unix_nano)),
            )],
            histograms: Vec::new(),
            exemplars: Vec::new(),
            metadata: metadata.cloned(),
        });
    }
    Ok(out)
}
