use super::Labels;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct DeltaKey {
    pub(crate) labels: Vec<DeltaLabel>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct DeltaLabel {
    pub(crate) name: String,
    pub(crate) value: String,
}

pub(crate) fn delta_key(labels: &Labels) -> DeltaKey {
    DeltaKey {
        labels: labels
            .iter()
            .map(|label| DeltaLabel {
                name: label.name.to_owned(),
                value: label.value.to_owned(),
            })
            .collect(),
    }
}
