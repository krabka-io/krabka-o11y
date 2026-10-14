use super::{LEGACY_CPU_METRIC_NAME, PprofProfile, intern_profile_string};

pub(crate) struct LegacyCpuProfile {
    pub(crate) profile: PprofProfile,
    pub(crate) metric_name: String,
}

struct LegacyProfileMetadata {
    unit: &'static str,
    metric: &'static str,
}

pub(crate) fn apply_legacy_profile_suffix(profile: PprofProfile, suffix: &str) -> LegacyCpuProfile {
    let metadata = match suffix {
        "alloc_objects" | "inuse_objects" => LegacyProfileMetadata {
            unit: "count",
            metric: "memory",
        },
        "alloc_space" | "inuse_space" => LegacyProfileMetadata {
            unit: "bytes",
            metric: "memory",
        },
        _ => {
            return LegacyCpuProfile {
                profile,
                metric_name: LEGACY_CPU_METRIC_NAME.to_string(),
            };
        }
    };
    let mut profile = profile.into_inner();
    let type_ref = intern_profile_string(&mut profile.string_table, suffix);
    let unit_ref = intern_profile_string(&mut profile.string_table, metadata.unit);
    profile.sample_type = vec![krabka_pprof::proto::ValueType {
        r#type: type_ref,
        unit: unit_ref,
    }];
    profile.period_type = None;
    profile.period = 0;
    profile.default_sample_type = type_ref;
    LegacyCpuProfile {
        profile: PprofProfile::from(profile),
        metric_name: metadata.metric.to_string(),
    }
}
