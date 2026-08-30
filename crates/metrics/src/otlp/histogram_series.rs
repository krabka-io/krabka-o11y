use super::*;

pub(crate) fn histogram_series(
    metric: &Metric,
    histogram: &Histogram,
    resource_attributes: &[KeyValue],
    strategy: TranslationStrategy,
    mut accumulator: Option<&mut DeltaAccumulator>,
) -> Result<Vec<DecodedSeries>, OtlpError> {
    let name = translated_metric_name(metric, strategy, false);
    let metadata = metric_metadata(metric, &name, "histogram");
    let mut out = Vec::new();
    for point in &histogram.data_points {
        let mut point_series =
            classic_histogram_series(&name, point, resource_attributes, Some(&metadata))?;
        if histogram.aggregation_temporality == AggregationTemporality::Delta as i32 {
            let Some(accumulator) = accumulator.as_deref_mut() else {
                return Err(OtlpError::DeltaUnsupported(metric.name.clone()));
            };
            accumulate_delta_float_series(
                &mut point_series,
                point.start_time_unix_nano,
                accumulator,
            );
        } else if histogram.aggregation_temporality != AggregationTemporality::Cumulative as i32
            && histogram.aggregation_temporality != AggregationTemporality::Unspecified as i32
        {
            return Err(OtlpError::DeltaUnsupported(metric.name.clone()));
        }
        out.extend(point_series);
    }
    Ok(out)
}
