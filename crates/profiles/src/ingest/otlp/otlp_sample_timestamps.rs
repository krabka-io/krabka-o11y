use super::*;

pub(crate) fn otlp_sample_timestamps(
    profile: &pb::otlp_profiles::Profile,
) -> Result<Vec<Vec<i64>>, ProfilesError> {
    profile
        .samples
        .iter()
        .map(|sample| {
            sample
                .timestamps_unix_nano
                .iter()
                .map(|timestamp| {
                    i64::try_from(*timestamp).map_err(|_| {
                        ProfilesError::Invalid("OTLP sample timestamp overflows i64".to_string())
                    })
                })
                .collect()
        })
        .collect()
}
