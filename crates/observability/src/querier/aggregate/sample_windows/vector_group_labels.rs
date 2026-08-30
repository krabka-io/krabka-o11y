use super::*;

pub(crate) fn vector_group_labels(labels: &Labels, grouping: Option<&VectorGrouping>) -> Labels {
    match grouping {
        Some(VectorGrouping::By(names)) => names
            .iter()
            .filter_map(|name| labels.get(name).map(|value| (name.clone(), value.clone())))
            .collect(),
        Some(VectorGrouping::Without(names)) => labels
            .iter()
            .filter(|(name, _)| !names.contains(name))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
        None => Labels::new(),
    }
}
