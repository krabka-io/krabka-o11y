use super::*;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct DeltaKey {
    pub(crate) labels: Vec<(String, String)>,
}

pub(crate) fn delta_key(labels: &Labels) -> DeltaKey {
    DeltaKey {
        labels: labels
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
    }
}
