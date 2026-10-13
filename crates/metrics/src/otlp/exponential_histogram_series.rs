use super::{
    DecodedSeries, DeltaAccumulator, ExponentialHistogram, HistogramFamily, HistogramScope,
    OtlpError, exemplars_from_exponential_histogram_point, exponential_histogram_to_native,
    histogram_points::decode_histogram_points, labels, nanos_to_millis,
};
use crate::wire::DecodedHistogramSample;

pub(crate) fn exponential_histogram_series(
    scope: &HistogramScope<'_>,
    histogram: &ExponentialHistogram,
    mut accumulator: Option<&mut DeltaAccumulator>,
) -> Result<Vec<DecodedSeries>, OtlpError> {
    decode_histogram_points(scope, &histogram.data_points, |point, family| {
        let HistogramFamily { name, metadata } = family;
        let labels = labels(
            &name,
            scope.resource_attributes,
            &point.attributes,
            None,
            scope.strategy,
        );
        let mut native_histogram = exponential_histogram_to_native(point)?;
        if let Some(accumulator) =
            scope.point_accumulator(histogram.aggregation_temporality, &mut accumulator)?
        {
            native_histogram = accumulator.accumulate_histogram(
                &scope.metric.name,
                &labels,
                point.start_time_unix_nano,
                native_histogram,
            )?;
        }
        Ok(vec![DecodedSeries {
            labels,
            samples: Vec::new(),
            histograms: vec![DecodedHistogramSample {
                timestamp_ms: nanos_to_millis(point.time_unix_nano),
                histogram: native_histogram,
            }],
            exemplars: exemplars_from_exponential_histogram_point(point, scope.strategy),
            metadata: Some(metadata.clone()),
        }])
    })
}
