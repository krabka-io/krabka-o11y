use super::PprofProfile;

pub(crate) fn apply_query_sample_rate(profile: PprofProfile, sample_rate: u32) -> PprofProfile {
    let mut profile = profile.into_inner();
    profile.period = (1_000_000_000_i64 / i64::from(sample_rate)).max(1);
    PprofProfile::from(profile)
}
