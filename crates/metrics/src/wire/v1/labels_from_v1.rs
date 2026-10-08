use super::{HashSet, Labels, WireError, pb};

pub(crate) fn labels_from_v1(labels: Vec<pb::v1::Label>) -> Result<Labels, WireError> {
    let mut names = HashSet::with_capacity(labels.len());
    for label in &labels {
        if !names.insert(label.name.as_str()) {
            return Err(WireError::Invalid(format!(
                "duplicate label `{}`",
                label.name
            )));
        }
    }
    Ok(labels
        .into_iter()
        .map(|label| (label.name, label.value))
        .collect())
}
