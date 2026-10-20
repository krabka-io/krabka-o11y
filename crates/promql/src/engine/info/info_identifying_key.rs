use krabka_metrics::MetricLabelPair;

use super::Labels;

pub(crate) fn info_identifying_key(labels: &Labels) -> Option<String> {
    if labels.get_value("job").is_none() && labels.get_value("instance").is_none() {
        return None;
    }
    let empty = crate::PromqlString::default();
    Some(
        Labels::from_pairs([
            MetricLabelPair::new("job", labels.get_value("job").unwrap_or(&empty).clone()),
            MetricLabelPair::new(
                "instance",
                labels.get_value("instance").unwrap_or(&empty).clone(),
            ),
        ])
        .order_key(),
    )
}
