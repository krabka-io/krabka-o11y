use super::{BTreeSet, LabelModifier, Labels};

pub(crate) fn aggregate_labels(input: &Labels, modifier: Option<&LabelModifier>) -> Labels {
    let mut labels = Labels::new();
    match modifier {
        Some(LabelModifier::Include(include)) => {
            for name in &include.labels {
                if let Some(value) = input.get_value(name) {
                    labels.insert(name, value);
                }
            }
        }
        Some(LabelModifier::Exclude(exclude)) => {
            let excluded = exclude.labels.iter().collect::<BTreeSet<_>>();
            for pair in input.iter() {
                if pair.name == "__name__" || excluded.contains(pair.name) {
                    continue;
                }
                labels.insert(pair.name, pair.value);
            }
        }
        None => {}
    }
    labels
}
