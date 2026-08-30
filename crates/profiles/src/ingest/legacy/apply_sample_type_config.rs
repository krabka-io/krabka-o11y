use super::{PprofProfile, SampleTypeConfig, intern_profile_string};

pub(crate) fn apply_sample_type_config(
    profile: PprofProfile,
    config: &SampleTypeConfig,
) -> PprofProfile {
    let mut profile = profile.into_inner();
    let Some(sample_type) = profile.sample_type.first().copied() else {
        return PprofProfile::from(profile);
    };
    let sample_type_name = config
        .display_name
        .as_deref()
        .filter(|value| !value.is_empty())
        .map_or(sample_type.r#type, |value| {
            intern_profile_string(&mut profile.string_table, value)
        });
    let sample_unit = config
        .units
        .as_deref()
        .filter(|value| !value.is_empty())
        .map_or(sample_type.unit, |value| {
            intern_profile_string(&mut profile.string_table, value)
        });
    if let Some(first) = profile.sample_type.first_mut() {
        first.r#type = sample_type_name;
        first.unit = sample_unit;
    }
    profile.period_type = Some(krabka_pprof::proto::ValueType {
        r#type: sample_type_name,
        unit: sample_unit,
    });
    profile.default_sample_type = sample_type_name;
    PprofProfile::from(profile)
}
