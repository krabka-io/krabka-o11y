use super::{LEGACY_CPU_METRIC_NAME, PprofProfile, intern_profile_string};

pub(crate) fn apply_legacy_profile_suffix(
    profile: PprofProfile,
    suffix: &str,
) -> (PprofProfile, String) {
    let (unit, metric) = match suffix {
        "alloc_objects" | "inuse_objects" => ("count", "memory"),
        "alloc_space" | "inuse_space" => ("bytes", "memory"),
        _ => return (profile, LEGACY_CPU_METRIC_NAME.to_string()),
    };
    let mut profile = profile.into_inner();
    let type_ref = intern_profile_string(&mut profile.string_table, suffix);
    let unit_ref = intern_profile_string(&mut profile.string_table, unit);
    profile.sample_type = vec![krabka_pprof::proto::ValueType {
        r#type: type_ref,
        unit: unit_ref,
    }];
    profile.period_type = None;
    profile.period = 0;
    profile.default_sample_type = type_ref;
    (PprofProfile::from(profile), metric.to_string())
}
