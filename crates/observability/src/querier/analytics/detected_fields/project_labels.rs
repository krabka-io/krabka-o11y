use super::*;

pub(crate) fn project_labels(labels: &Labels, target_labels: &[String]) -> Labels {
    target_labels
        .iter()
        .filter_map(|name| labels.get(name).map(|value| (name.clone(), value.clone())))
        .collect()
}
