use super::*;

/// Split one multi-value pprof into one `DecodedProfile` per `sample_type[]`.
///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub fn split_sample_types(raw: &RawProfile) -> Result<Vec<DecodedProfile>, ProfilesError> {
    let name = raw
        .labels
        .get("__name__")
        .filter(|name| !name.is_empty())
        .ok_or_else(|| ProfilesError::Invalid("missing __name__".to_string()))?
        .to_string();
    let (period_type, period_unit) = raw.profile.period_type_strings();
    if period_type.is_empty() || period_unit.is_empty() {
        return Err(ProfilesError::Decode(
            "profile period_type is missing or invalid".to_string(),
        ));
    }

    let sample_types = raw.profile.sample_types();
    let timestamp_ns = raw.profile.inner().time_nanos;
    let location_refs = raw
        .profile
        .inner()
        .location
        .iter()
        .enumerate()
        .map(|(idx, location)| {
            let idx = u32::try_from(idx).map_err(|err| {
                ProfilesError::Decode(format!("location index does not fit u32: {err}"))
            })?;
            Ok((location.id, idx))
        })
        .collect::<Result<HashMap<_, _>, ProfilesError>>()?;
    let mut out = Vec::with_capacity(sample_types.len());
    for (idx, (sample_type, sample_unit)) in sample_types.into_iter().enumerate() {
        if sample_type.is_empty() || sample_unit.is_empty() {
            return Err(ProfilesError::Decode(format!(
                "sample_type[{idx}] is missing or invalid"
            )));
        }

        let profile_type = ProfileType {
            name: name.clone(),
            sample_type: sample_type.clone(),
            sample_unit: sample_unit.clone(),
            period_type: period_type.clone(),
            period_unit: period_unit.clone(),
            delta: raw.delta,
        }
        .to_string();

        let mut labels = raw.labels.clone();
        labels.insert("__profile_type__", profile_type.clone());
        labels.insert("__period_type__", period_type.clone());
        labels.insert("__period_unit__", period_unit.clone());
        labels.insert("__type__", sample_type.clone());
        labels.insert("__unit__", sample_unit.clone());
        if let Some(service_name) = raw.labels.get("service_name") {
            labels.insert("__service_name__", service_name.to_string());
        }

        let mut groups = BTreeMap::<Vec<(String, String)>, (Labels, Vec<DecodedSample>)>::new();
        for (sample_idx, sample) in raw.profile.samples().iter().enumerate() {
            let value = sample
                .value
                .get(idx)
                .copied()
                .ok_or_else(|| ProfilesError::Decode(format!("sample value[{idx}] missing")))?;
            let timestamp_ns = raw
                .sample_timestamps_ns
                .get(sample_idx)
                .and_then(|timestamps| timestamps.get(idx))
                .copied()
                .unwrap_or(timestamp_ns);
            let stacktrace_location_refs = sample
                .location_id
                .iter()
                .map(|location| {
                    location_refs.get(location).copied().ok_or_else(|| {
                        ProfilesError::Decode(format!(
                            "sample references missing location id {location}"
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let sample_labels = labels_with_sample_labels(&labels, &raw.profile, sample);
            let key = labels_key(&sample_labels);
            groups
                .entry(key)
                .or_insert((sample_labels, Vec::new()))
                .1
                .push(DecodedSample {
                    stacktrace_location_refs,
                    value,
                    timestamp_ns,
                    span_id: raw.sample_span_ids.get(sample_idx).copied().flatten(),
                    trace_id: raw.sample_trace_ids.get(sample_idx).cloned().flatten(),
                });
        }

        out.extend(
            groups
                .into_values()
                .map(|(labels, samples)| DecodedProfile {
                    labels,
                    profile_type: profile_type.clone(),
                    samples,
                }),
        );
    }

    Ok(out)
}
