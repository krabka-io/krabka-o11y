use super::{BTreeSet, BinModifier, LabelModifier, Labels, is_result_metadata_label, labels_key};

pub(crate) fn binary_match_key(labels: &Labels, modifier: Option<&BinModifier>) -> String {
    let mut key_labels = Labels::new();
    match modifier.and_then(|modifier| modifier.matching.as_ref()) {
        Some(LabelModifier::Include(include)) => {
            for name in &include.labels {
                if let Some(value) = labels.get(name) {
                    key_labels.insert(name, value);
                }
            }
        }
        Some(LabelModifier::Exclude(exclude)) => {
            let excluded = exclude.labels.iter().collect::<BTreeSet<_>>();
            for (name, value) in labels.iter() {
                // Only `__name__` here, deliberately: with an explicit
                // `ignoring (...)` clause Prometheus leaves `__type__` and
                // `__unit__` in the match key, so series differing only in
                // those do *not* pair up. Default matching below drops all
                // three. Making the two agree fails the upstream
                // `type_and_unit.test` case
                // `... / ignoring(group) ...`, which must yield no samples.
                if name == "__name__" || excluded.contains(name) {
                    continue;
                }
                key_labels.insert(name, value);
            }
        }
        None => {
            for (name, value) in labels.iter() {
                if is_result_metadata_label(name) {
                    continue;
                }
                key_labels.insert(name, value);
            }
        }
    }
    labels_key(&key_labels)
}
