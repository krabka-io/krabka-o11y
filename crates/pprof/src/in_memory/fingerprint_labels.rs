use super::Labels;

pub(crate) fn fingerprint_labels(labels: &[(String, String)]) -> u64 {
    let mut canonical = Labels::new();
    for (name, value) in labels {
        canonical.insert(name.clone(), value.clone());
    }
    canonical.fingerprint()
}
