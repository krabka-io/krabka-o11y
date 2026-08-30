use super::{
    Labels, ProfilesError, RawProfile, hex_lower, otlp_profile_to_pprof, otlp_sample_links,
    otlp_sample_timestamps, pb, profile_labels, resolve_service_name,
};

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub fn decode_otlp(
    req: &pb::otlp_profiles::ExportProfilesServiceRequest,
) -> Result<Vec<RawProfile>, ProfilesError> {
    let dict = req
        .dictionary
        .as_ref()
        .ok_or_else(|| ProfilesError::Invalid("OTLP profiles missing dictionary".to_string()))?;
    let mut out = Vec::new();

    for resource_profiles in &req.resource_profiles {
        let service_name = resolve_service_name(resource_profiles);
        for scope_profiles in &resource_profiles.scope_profiles {
            for profile in &scope_profiles.profiles {
                let sample_timestamps_ns = otlp_sample_timestamps(profile)?;
                let (sample_span_ids, sample_trace_ids) = otlp_sample_links(profile, dict)?;
                let profile_labels = profile_labels(profile, dict)?;
                let profile_id =
                    (!profile.profile_id.is_empty()).then(|| hex_lower(&profile.profile_id));
                let profile = otlp_profile_to_pprof(profile, dict)?;
                let mut labels = Labels::new();
                labels.insert("service_name", service_name.clone());
                if let Some(profile_id) = profile_id {
                    labels.insert("__profile_id__", profile_id);
                }
                for (name, value) in profile_labels {
                    labels.insert(name, value);
                }
                if let Some((name, _)) = profile.sample_types().first() {
                    labels.insert("__name__", name.clone());
                }
                out.push(RawProfile {
                    labels,
                    profile,
                    delta: false,
                    sample_timestamps_ns,
                    sample_span_ids,
                    sample_trace_ids,
                });
            }
        }
    }

    Ok(out)
}
