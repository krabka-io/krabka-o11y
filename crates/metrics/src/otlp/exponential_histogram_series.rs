use super::{
    AggregationTemporality, DecodedSeries, DeltaAccumulator, ExponentialHistogram, HistogramFamily,
    KeyValue, Metric, OtlpError, TranslationStrategy, exemplars_from_exponential_histogram_point,
    exponential_histogram_to_native, labels, nanos_to_millis,
};

pub(crate) fn exponential_histogram_series(
    metric: &Metric,
    histogram: &ExponentialHistogram,
    resource_attributes: &[KeyValue],
    strategy: TranslationStrategy,
    mut accumulator: Option<&mut DeltaAccumulator>,
) -> Result<Vec<DecodedSeries>, OtlpError> {
    let HistogramFamily { name, metadata } = HistogramFamily::of(metric, strategy);
    let mut out = Vec::new();
    for point in &histogram.data_points {
        let labels = labels(
            &name,
            resource_attributes,
            &point.attributes,
            None,
            strategy,
        );
        let mut native_histogram = exponential_histogram_to_native(point)?;
        if histogram.aggregation_temporality == AggregationTemporality::Delta as i32 {
            let Some(accumulator) = accumulator.as_deref_mut() else {
                return Err(OtlpError::DeltaUnsupported(metric.name.clone()));
            };
            native_histogram = accumulator.accumulate_histogram(
                &metric.name,
                &labels,
                point.start_time_unix_nano,
                native_histogram,
            )?;
        } else if histogram.aggregation_temporality != AggregationTemporality::Cumulative as i32
            && histogram.aggregation_temporality != AggregationTemporality::Unspecified as i32
        {
            return Err(OtlpError::DeltaUnsupported(metric.name.clone()));
        }
        out.push(DecodedSeries {
            labels,
            samples: Vec::new(),
            histograms: vec![(nanos_to_millis(point.time_unix_nano), native_histogram)],
            exemplars: exemplars_from_exponential_histogram_point(point, strategy),
            metadata: Some(metadata.clone()),
        });
    }
    Ok(out)
}
