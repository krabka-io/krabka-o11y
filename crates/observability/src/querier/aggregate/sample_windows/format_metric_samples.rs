use super::*;

pub(crate) fn format_metric_samples(
    samples: MetricSamples,
    query: &MetricQuery,
) -> FormattedMetricSeries {
    let samples = if let Some(grouping) = &query.range_grouping {
        group_range_samples(samples, grouping)
    } else {
        samples
    };

    if let Some(vector_aggregation) = &query.vector_aggregation {
        let mut series = aggregate_vector_samples(samples, query, vector_aggregation)
            .into_iter()
            .map(|(labels, values)| {
                (
                    labels,
                    values
                        .into_iter()
                        .map(|(time, value)| [time.to_string(), format_metric_value(value)])
                        .collect(),
                )
            })
            .collect::<FormattedMetricSeries>();
        sort_formatted_vector_samples(&mut series, &vector_aggregation.op);
        return series;
    }

    samples
        .into_iter()
        .map(|(labels, values)| {
            (
                labels,
                values
                    .into_iter()
                    .map(|(time, value)| {
                        [
                            time.to_string(),
                            format_metric_value(range_sample_value(value, query)),
                        ]
                    })
                    .collect(),
            )
        })
        .collect()
}
