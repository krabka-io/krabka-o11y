use super::{Labels, is_result_metadata_label};

pub(crate) fn labels_without_metric_name(input: &Labels) -> Labels {
    let mut labels = Labels::new();
    for (name, value) in input.iter() {
        if !is_result_metadata_label(name) {
            labels.insert(name, value);
        }
    }
    labels
}
