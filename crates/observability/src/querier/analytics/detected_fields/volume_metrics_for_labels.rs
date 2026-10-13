use super::{BTreeMap, Labels, VolumeAggregateBy, VolumeParams, project_labels};

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
                .map(|name| {
                    let mut projected = BTreeMap::new();
                    projected.insert(name.clone(), String::new());
                    projected
                })
                .collect(),
            None => labels
                .keys()
                .map(|name| {
                    let mut projected = BTreeMap::new();
                    projected.insert(name.clone(), String::new());
                    projected
                })
                .collect(),
        },
    }
}
