use super::{LabelModifier, Labels};

/// The labels a sharded aggregation groups a series by.
///
/// This is the engine's grouping, except that a `by` clause never keeps
/// `__name__` and keeps each value as its UTF-8 text.
pub(crate) fn aggregate_labels(input: &Labels, modifier: Option<&LabelModifier>) -> Labels {
    let grouped = crate::engine::aggregate_labels(input, modifier);
    if !matches!(modifier, Some(LabelModifier::Include(_))) {
        return grouped;
    }
    let mut labels = Labels::new();
    for (name, value) in grouped.iter() {
        if name != "__name__" {
            labels.insert(name, value.as_str());
        }
    }
    labels
}
