use super::{
    FormattedMetricSeries, MetricQuery, MetricSamples, aggregate_vector_samples,
    format_metric_value, group_range_samples, range_sample_value, sort_formatted_vector_samples,
};

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
        let aggregated = aggregate_vector_samples(samples, query, vector_aggregation);
        let mut series = aggregated
            .keys()
            .map(|labels| {
                let values = &aggregated[labels];
                super::FormattedMetricSeriesEntry {
                    labels: labels.clone(),
                    values: values
                        .keys()
                        .map(|time| [time.to_string(), format_metric_value(values[time])])
                        .collect(),
                }
            })
            .collect::<FormattedMetricSeries>();
        sort_formatted_vector_samples(&mut series, &vector_aggregation.op);
        return series;
    }

    samples
        .into_iter()
        .map(|(labels, values)| super::FormattedMetricSeriesEntry {
            labels,
            values: values
                .into_iter()
                .map(|(time, mut value)| {
                    [
                        time.to_string(),
                        format_metric_value(range_sample_value(&mut value, query)),
                    ]
                })
                .collect(),
        })
        .collect()
}
