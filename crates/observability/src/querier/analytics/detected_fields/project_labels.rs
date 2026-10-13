use super::Labels;

pub(crate) fn project_labels(labels: &Labels, target_labels: &[String]) -> Labels {
    let mut projected = Labels::new();
    for name in target_labels {
        if let Some(value) = labels.get(name) {
            projected.insert(name.clone(), value.clone());
        }
    }
    projected
}
