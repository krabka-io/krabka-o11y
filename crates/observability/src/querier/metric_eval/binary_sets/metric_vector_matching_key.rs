use super::*;

pub(crate) fn metric_vector_matching_key(
    labels: &Labels,
    matching: Option<&MetricVectorMatching>,
) -> Labels {
    match matching {
        None => labels.clone(),
        Some(MetricVectorMatching::On { labels: names, .. }) => names
            .iter()
            .filter_map(|name| labels.get(name).map(|value| (name.clone(), value.clone())))
            .collect(),
        Some(MetricVectorMatching::Ignoring { labels: names, .. }) => labels
            .iter()
            .filter(|(name, _)| !names.contains(name))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
    }
}
