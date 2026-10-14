use super::{Labels, pb};

pub(crate) fn remote_read_labels(labels: &Labels) -> Vec<pb::v1::Label> {
    labels
        .iter()
        .map(|pair| pb::v1::Label {
            name: pair.name.clone(),
            value: pair.value.as_bytes().to_vec(),
        })
        .collect()
}
