use super::*;

pub(crate) fn apply_query_time(
    profile: PprofProfile,
    query: &IngestQuery,
) -> Result<PprofProfile, ProfilesError> {
    if profile.inner().time_nanos != 0 {
        return Ok(profile);
    }
    let Some(timestamp_ms) = query.until_ms.or(query.from_ms) else {
        return Ok(profile);
    };
    let time_nanos = timestamp_ms.checked_mul(1_000_000).ok_or_else(|| {
        ProfilesError::Invalid(format!(
            "ingest timestamp overflows nanoseconds: {timestamp_ms}"
        ))
    })?;
    let mut profile = profile.into_inner();
    profile.time_nanos = time_nanos;
    Ok(PprofProfile::from(profile))
}
