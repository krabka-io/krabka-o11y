use super::{
    IngestQuery, LEGACY_CPU_METRIC_NAME, PprofProfile, apply_legacy_cpu_mapping,
    apply_legacy_profile_suffix,
};

/// A stack-shaped legacy profile re-typed as Pyroscope's default CPU profile,
/// paired with the metric name that goes with it.
pub(crate) fn legacy_cpu_profile(
    profile: PprofProfile,
    query: &IngestQuery,
) -> (PprofProfile, String) {
    if let Some(suffix) = &query.profile_type_suffix {
        return apply_legacy_profile_suffix(profile, suffix);
    }
    (
        apply_legacy_cpu_mapping(profile, query.sample_rate),
        LEGACY_CPU_METRIC_NAME.to_string(),
    )
}
