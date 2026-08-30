use super::*;

pub(crate) fn format_metric_vector_matching(
    matching: &MetricVectorMatching,
) -> FormattedMetricVectorMatching {
    match matching {
        MetricVectorMatching::On { labels, group } => FormattedMetricVectorMatching {
            text: format_metric_vector_matching_text("on", labels, group.as_ref()),
            has_group: group.is_some(),
        },
        MetricVectorMatching::Ignoring { labels, group } => FormattedMetricVectorMatching {
            text: format_metric_vector_matching_text("ignoring", labels, group.as_ref()),
            has_group: group.is_some(),
        },
    }
}
