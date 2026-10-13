use super::{Labels, VectorGrouping};

pub(crate) fn vector_group_labels(labels: &Labels, grouping: Option<&VectorGrouping>) -> Labels {
    match grouping {
        Some(VectorGrouping::By(names)) => {
            let mut grouped = Labels::new();
            for name in names {
                if let Some(value) = labels.get(name) {
                    grouped.insert(name.clone(), value.clone());
                }
            }
            grouped
        }
        Some(VectorGrouping::Without(names)) => {
            let mut grouped = Labels::new();
            for entry in labels {
                if !names.contains(entry.0) {
                    grouped.insert(entry.0.clone(), entry.1.clone());
                }
            }
            grouped
        }
        None => Labels::new(),
    }
}
