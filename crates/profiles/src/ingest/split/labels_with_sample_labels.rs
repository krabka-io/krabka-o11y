use super::Labels;

pub(crate) fn labels_with_sample_labels(
    base: &Labels,
    profile: &krabka_pprof::PprofProfile,
    sample: &krabka_pprof::proto::Sample,
) -> Labels {
    let mut labels = base.clone();
    for label in &sample.label {
        if label.str <= 0 {
            continue;
        }
        let Some(name) = profile.string(label.key) else {
            continue;
        };
        if labels.get(name).is_some() {
            continue;
        }
        if let Some(value) = profile.string(label.str) {
            labels.insert(name.to_string(), value.to_string());
        }
    }
    labels
}
