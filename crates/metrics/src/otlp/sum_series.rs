use super::{Metric, Sum, KeyValue, TranslationStrategy, DeltaAccumulator, DecodedSeries, OtlpError, translated_metric_name, AggregationTemporality, delta_sum_series, metric_metadata, sum_metadata_type, ExemplarPolicy, scalar_series};

pub(crate) fn sum_series(
    metric: &Metric,
    sum: &Sum,
    resource_attributes: &[KeyValue],
    strategy: TranslationStrategy,
    accumulator: Option<&mut DeltaAccumulator>,
) -> Result<Vec<DecodedSeries>, OtlpError> {
    let name = translated_metric_name(metric, strategy, sum.is_monotonic);

    if sum.aggregation_temporality == AggregationTemporality::Delta as i32 {
        let Some(accumulator) = accumulator else {
            return Err(OtlpError::DeltaUnsupported(metric.name.clone()));
        };
        return sum
            .data_points
            .iter()
            .map(|point| {
                delta_sum_series(
                    &name,
                    point,
                    resource_attributes,
                    accumulator,
                    Some(metric_metadata(metric, &name, sum_metadata_type(sum))),
                )
            })
            .collect();
    }

    if sum.aggregation_temporality != AggregationTemporality::Cumulative as i32
        && sum.aggregation_temporality != AggregationTemporality::Unspecified as i32
    {
        return Err(OtlpError::DeltaUnsupported(metric.name.clone()));
    }

    sum.data_points
        .iter()
        .map(|point| {
            let exemplar_policy = if sum.is_monotonic {
                ExemplarPolicy::Keep
            } else {
                ExemplarPolicy::Drop
            };
            scalar_series(
                &name,
                point,
                resource_attributes,
                Some(metric_metadata(metric, &name, sum_metadata_type(sum))),
                exemplar_policy,
            )
        })
        .collect()
}
