use super::{pb, Labels, WireError, HashSet};

pub(crate) fn labels_from_v1(labels: &[pb::v1::Label]) -> Result<Labels, WireError> {
    let mut names = HashSet::with_capacity(labels.len());
    labels
        .iter()
        .map(|label| {
            if !names.insert(label.name.as_str()) {
                return Err(WireError::Invalid(format!(
                    "duplicate label `{}`",
                    label.name
                )));
            }
            Ok((label.name.clone(), label.value.clone()))
        })
        .collect()
}
