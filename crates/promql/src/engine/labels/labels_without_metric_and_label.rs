use super::*;

pub(crate) fn labels_without_metric_and_label(input: &Labels, drop_label: &str) -> Labels {
    let mut labels = Labels::new();
    for (name, value) in input.iter() {
        if !is_result_metadata_label(name) && name != drop_label {
            labels.insert(name, value);
        }
    }
    labels
}
