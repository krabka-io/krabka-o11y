use super::{NumberDataPoint, KeyValue, DeltaAccumulator, DecodedMetadata, DecodedSeries, OtlpError, number_value, labels, DecodedSample, nanos_to_millis, exemplars_from_number_point};

pub(crate) fn delta_sum_series(
    name: &str,
    point: &NumberDataPoint,
    resource_attributes: &[KeyValue],
    accumulator: &mut DeltaAccumulator,
    metadata: Option<DecodedMetadata>,
) -> Result<DecodedSeries, OtlpError> {
    let delta = number_value(point)
        .ok_or_else(|| OtlpError::Invalid(name.into(), "missing number datapoint value".into()))?;
    let labels = labels(name, resource_attributes, &point.attributes, None);
    let cumulative = accumulator.accumulate_sum(&labels, point.start_time_unix_nano, delta);
    Ok(DecodedSeries {
        labels,
        samples: vec![DecodedSample::with_start_timestamp(
            nanos_to_millis(point.time_unix_nano),
            cumulative,
            (point.start_time_unix_nano != 0)
                .then_some(nanos_to_millis(point.start_time_unix_nano)),
        )],
        histograms: Vec::new(),
        exemplars: exemplars_from_number_point(point),
        metadata,
    })
}
