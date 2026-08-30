use super::*;

pub(crate) fn copy_group_labels(labels: &mut Labels, one_side: &Labels, group_labels: &[String]) {
    for name in group_labels {
        if is_result_metadata_label(name) {
            continue;
        }
        if let Some(value) = one_side.get(name) {
            labels.insert(name, value);
        }
    }
}
