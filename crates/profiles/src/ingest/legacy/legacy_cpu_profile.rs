use super::{
    IngestQuery, LEGACY_CPU_METRIC_NAME, LegacyCpuProfile, PprofProfile, apply_legacy_cpu_mapping,
    apply_legacy_profile_suffix,
};

/// A stack-shaped legacy profile re-typed as Pyroscope's default CPU profile,
/// paired with the metric name that goes with it.
pub(crate) fn legacy_cpu_profile(profile: PprofProfile, query: &IngestQuery) -> LegacyCpuProfile {
    if let Some(suffix) = &query.profile_type_suffix {
        return apply_legacy_profile_suffix(profile, suffix);
    }
    LegacyCpuProfile {
        profile: apply_legacy_cpu_mapping(profile, query.sample_rate),
        metric_name: LEGACY_CPU_METRIC_NAME.to_string(),
    }
}
