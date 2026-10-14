use super::{Labels, MetricVectorMatching};

pub(crate) fn metric_vector_matching_key(
    labels: &Labels,
    matching: Option<&MetricVectorMatching>,
) -> Labels {
    match matching {
        None => labels.clone(),
        Some(MetricVectorMatching::On { labels: names, .. }) => {
            let mut key = Labels::new();
            for name in names {
                if let Some(value) = labels.get(name) {
                    key.insert(name.clone(), value.clone());
                }
            }
            key
        }
        Some(MetricVectorMatching::Ignoring { labels: names, .. }) => {
            let mut key = Labels::new();
            for entry in labels {
                if !names.contains(entry.0) {
                    key.insert(entry.0.clone(), entry.1.clone());
                }
            }
            key
        }
    }
}
