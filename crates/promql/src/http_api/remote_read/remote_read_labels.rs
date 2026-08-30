use super::*;

pub(crate) fn remote_read_labels(labels: &Labels) -> Vec<pb::v1::Label> {
    labels
        .iter()
        .map(|(name, value)| pb::v1::Label {
            name: name.clone(),
            value: value.clone(),
        })
        .collect()
}
