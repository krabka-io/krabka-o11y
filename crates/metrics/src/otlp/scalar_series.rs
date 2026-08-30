use super::*;

pub(crate) fn scalar_series(
    name: &str,
    point: &NumberDataPoint,
    resource_attributes: &[KeyValue],
    metadata: Option<DecodedMetadata>,
    exemplar_policy: ExemplarPolicy,
) -> Result<DecodedSeries, OtlpError> {
    let value = number_value(point)
        .ok_or_else(|| OtlpError::Invalid(name.into(), "missing number datapoint value".into()))?;
    let exemplars = match exemplar_policy {
        ExemplarPolicy::Keep => exemplars_from_number_point(point),
        ExemplarPolicy::Drop => Vec::new(),
    };
    Ok(DecodedSeries {
        labels: labels(name, resource_attributes, &point.attributes, None),
        samples: vec![DecodedSample::with_start_timestamp(
            nanos_to_millis(point.time_unix_nano),
            value,
            (point.start_time_unix_nano != 0)
                .then_some(nanos_to_millis(point.start_time_unix_nano)),
        )],
        histograms: Vec::new(),
        exemplars,
        metadata,
    })
}
