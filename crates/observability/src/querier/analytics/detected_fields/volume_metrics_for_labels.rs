use super::{BTreeMap, Labels, VolumeAggregateBy, VolumeParams, project_labels};

fn labels_aggregate_by_name(name: &str) -> Labels {
    BTreeMap::from([(name.to_string(), String::new())])
}

pub(crate) fn volume_metrics_for_labels(labels: &Labels, params: &VolumeParams) -> Vec<Labels> {
    match params.aggregate_by {
        VolumeAggregateBy::Series => {
            let labels = if let Some(target_labels) = &params.target_labels {
                project_labels(labels, target_labels)
            } else {
                labels.clone()
            };
            vec![labels]
        }
        VolumeAggregateBy::Labels => match &params.target_labels {
            Some(target_labels) => target_labels
                .iter()
                .filter(|name| labels.contains_key(*name))
                .map(|name| labels_aggregate_by_name(name))
                .collect(),
            None => labels
                .keys()
                .map(|name| labels_aggregate_by_name(name))
                .collect(),
        },
    }
}
