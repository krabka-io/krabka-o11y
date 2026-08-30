use super::{BTreeSet, Grouping};

/// Maps a `PromQL` [`Grouping`] onto the concrete grouping label columns.
///
/// The result is intersected with the labels present in the input. `by` keeps
/// the listed labels in their given order and drops each label that is not
/// present. `without` keeps every present label except the listed ones and
/// `__name__`.
pub(crate) fn resolve_group_labels(input_labels: &[String], grouping: &Grouping) -> Vec<String> {
    match grouping {
        Grouping::By(labels) => {
            let present: BTreeSet<&String> = input_labels.iter().collect();
            // Preserve the user's `by` order; drop labels absent from the input.
            let mut seen = BTreeSet::new();
            labels
                .iter()
                .filter(|name| present.contains(name) && seen.insert((*name).clone()))
                .cloned()
                .collect()
        }
        Grouping::Without(labels) => {
            let excluded: BTreeSet<&String> = labels.iter().collect();
            input_labels
                .iter()
                .filter(|name| name.as_str() != "__name__" && !excluded.contains(name))
                .cloned()
                .collect()
        }
    }
}
