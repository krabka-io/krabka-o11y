use super::Labels;

pub(crate) fn metadata_visible_labels(labels: &Labels) -> Labels {
    let mut labels = labels.clone();
    labels.remove("detected_level");
    labels
}
