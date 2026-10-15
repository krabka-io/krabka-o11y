use super::{
    DecodedSeries, DeltaAccumulator, ExponentialHistogram, ExtraLabel, Histogram,
    HistogramDataPoint, HistogramFamily, HistogramScope, OtlpError, PointFamily, ScalarPointSeries,
    ScalarSeries, ToPrimitive, accumulate_delta_float_series, append_count_and_sum,
    exemplars_for_bucket, exemplars_from_exponential_histogram_point,
    exemplars_from_histogram_point, exponential_histogram_to_native,
    histogram_points::decode_histogram_points, labels, nanos_to_millis,
};
use crate::wire::DecodedHistogramSample;

pub(crate) fn classic_histogram_series(
    point: &HistogramDataPoint,
    family: &PointFamily<'_>,
) -> Result<Vec<DecodedSeries>, OtlpError> {
    let PointFamily {
        name,
        resource_attributes,
        metadata,
        strategy,
    } = *family;
    if !point.bucket_counts.is_empty()
        && point.bucket_counts.len() != point.explicit_bounds.len() + 1
    {
        return Err(OtlpError::Invalid(
            name.into(),
            "bucket_counts length must be explicit_bounds length plus one".into(),
        ));
    }

    let scalar = ScalarPointSeries {
        resource_attributes,
        attributes: &point.attributes,
        strategy,
        time_unix_nano: point.time_unix_nano,
        start_time_unix_nano: point.start_time_unix_nano,
        metadata: metadata.cloned(),
    };
    let point_exemplars = exemplars_from_histogram_point(point, strategy);
    let mut out = Vec::new();
    let base_name = format!("{name}_bucket");
    let mut cumulative = 0_u64;
    for bucket in point.bucket_counts.iter().enumerate() {
        let idx = bucket.0;
        cumulative = cumulative.saturating_add(*bucket.1);
        let le = point
            .explicit_bounds
            .get(idx)
            .map_or_else(|| "+Inf".to_string(), ToString::to_string);
        out.push(scalar.series(ScalarSeries {
            name: &base_name,
            extra_label: Some(ExtraLabel {
                label_name: "le",
                label_value: &le,
            }),
            sample_value: cumulative.to_f64().unwrap_or(f64::MAX),
            exemplars: exemplars_for_bucket(&point_exemplars, point, idx),
        }));
    }

    append_count_and_sum(
        &mut out,
        &scalar,
        name,
        point.count.to_f64().unwrap_or(f64::MAX),
        point.sum,
    );
    Ok(out)
}

pub(crate) fn histogram_series(
    scope: &HistogramScope<'_>,
    histogram: &Histogram,
    mut accumulator: Option<&mut DeltaAccumulator>,
) -> Result<Vec<DecodedSeries>, OtlpError> {
    decode_histogram_points(scope, &histogram.data_points, |point, family| {
        let HistogramFamily { name, metadata } = family;
        let mut point_series = classic_histogram_series(
            point,
            &PointFamily {
                name,
                resource_attributes: scope.resource_attributes,
                metadata: Some(metadata),
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
        Ok(point_series)
    })
}

pub(crate) fn exponential_histogram_series(
    scope: &HistogramScope<'_>,
    histogram: &ExponentialHistogram,
    mut accumulator: Option<&mut DeltaAccumulator>,
) -> Result<Vec<DecodedSeries>, OtlpError> {
    decode_histogram_points(scope, &histogram.data_points, |point, family| {
        let HistogramFamily { name, metadata } = family;
        let labels = labels(
            name,
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
