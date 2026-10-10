use super::{
    DecodedSeries, DeltaAccumulator, Histogram, HistogramFamily, HistogramScope, OtlpError,
    PointFamily, accumulate_delta_float_series, classic_histogram_series,
};

pub(crate) fn histogram_series(
    scope: &HistogramScope<'_>,
    histogram: &Histogram,
    mut accumulator: Option<&mut DeltaAccumulator>,
) -> Result<Vec<DecodedSeries>, OtlpError> {
    let HistogramFamily { name, metadata } = HistogramFamily::of(scope.metric, scope.strategy);
    let mut out = Vec::new();
    for point in &histogram.data_points {
        let mut point_series = classic_histogram_series(
            point,
            &PointFamily {
                name: &name,
                resource_attributes: scope.resource_attributes,
                metadata: Some(&metadata),
                strategy: scope.strategy,
            },
        )?;
        if let Some(accumulator) =
            scope.point_accumulator(histogram.aggregation_temporality, &mut accumulator)?
        {
            accumulate_delta_float_series(
                &mut point_series,
                point.start_time_unix_nano,
                accumulator,
            );
        }
        out.extend(point_series);
    }
    Ok(out)
}
