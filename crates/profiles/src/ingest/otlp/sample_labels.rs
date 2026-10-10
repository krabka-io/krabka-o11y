use super::{ProfilesError, attribute_label, pb};
use crate::ingest::intern_profile_string;

pub(crate) fn sample_labels(
    sample: &pb::otlp_profiles::Sample,
    dict: &pb::otlp_profiles::ProfilesDictionary,
    strings: &mut Vec<String>,
) -> Result<Vec<krabka_pprof::proto::Label>, ProfilesError> {
    let mut labels = Vec::new();
    for attr_idx in &sample.attribute_indices {
        let (name, value) = attribute_label(*attr_idx, dict)?;
        let key = intern_profile_string(strings, &name);
        let value_idx = intern_profile_string(strings, &value);
        labels.push(krabka_pprof::proto::Label {
            key,
            str: value_idx,
            num: 0,
            num_unit: 0,
        });
    }
    Ok(labels)
}
