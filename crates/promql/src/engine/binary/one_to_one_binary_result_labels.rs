use super::{Labels, BinModifier, LabelModifier, is_result_metadata_label, BTreeSet, labels_without_metric_name};

pub(crate) fn one_to_one_binary_result_labels(input: &Labels, modifier: Option<&BinModifier>) -> Labels {
    match modifier.and_then(|modifier| modifier.matching.as_ref()) {
        Some(LabelModifier::Include(include)) => {
            let mut labels = Labels::new();
            for name in &include.labels {
                if is_result_metadata_label(name) {
                    continue;
                }
                if let Some(value) = input.get(name) {
                    labels.insert(name, value);
                }
            }
            labels
        }
        Some(LabelModifier::Exclude(exclude)) => {
            let excluded = exclude.labels.iter().collect::<BTreeSet<_>>();
            let mut labels = Labels::new();
            for (name, value) in input.iter() {
                if is_result_metadata_label(name) || excluded.contains(name) {
                    continue;
                }
                labels.insert(name, value);
            }
            labels
        }
        None => labels_without_metric_name(input),
    }
}
