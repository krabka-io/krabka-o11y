use super::{ProfilesError, attribute_label, pb};

pub(crate) fn profile_labels(
    profile: &pb::otlp_profiles::Profile,
    dict: &pb::otlp_profiles::ProfilesDictionary,
) -> Result<Vec<krabka_blockstore::LabelPair>, ProfilesError> {
    profile
        .attribute_indices
        .iter()
        .map(|idx| attribute_label(*idx, dict))
        .collect()
}
